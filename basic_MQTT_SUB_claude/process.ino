


#include "Arduino.h"
#include <WiFiClient.h>

#include <ESPmDNS.h>
#include <Update.h>
#include <WiFiMulti.h>
#include <ArduinoJson.h>
#include "ESP32MQTTClient.h"
#include <WebSerial.h>
#include <LittleFS.h>
#include <M5Unified.h>
#include <SPI.h>
#include <SD.h>
#include <time.h>


#include "isolation_forest_model.h"
#include "isolation_forest_runtime.h"
#include <Preferences.h>

//#define NO_DEBUG 
//#define   DEBUG_TELNET_PORT  
#define   DEBUG_SERIAL_PORT 


// *******************Enable and disable serial print **************
#ifdef DEBUG_SERIAL_PORT
    #define DEBUG_PRINTLN(x)      Serial.println (x)
    #define DEBUG_PRINT(x)        Serial.print (x)
    #define DEBUG_PRINTF(f,...)   Serial.printf(f,##__VA_ARGS__)
#endif

#ifdef DEBUG_TELNET_PORT
    #define    DEBUG_PRINTF(f,...)  TelnetPrintf(f,##__VA_ARGS__)
    #define    DEBUG_PRINTLN(x) 
    #define    DEBUG_PRINT(x)
#endif

#ifdef NO_DEBUG
    #define DEBUG_PRINT(x)
    #define DEBUG_PRINTLN(x)
    #define DEBUG_PRINTF(f,...)
#endif



/* ***************** Prototype Definitions************** */
void WhenRoomIsEmpty(const char* room, time_t startEpoch, time_t endEpoch) ;
void checkDailyRollover() ;
void checkOngoingToiletVisits() ;
void finalizeBedSession() ;
unsigned long getEpochTime() ;
void checkHourlyPatternImmediate(int hour)  ;
void maybeRecordWakeTime(time_t startEpoch, time_t endEpoch, int durationSec) ;
/* ***************** End Prototype Definitions ******************** */

const float ALPHA_PER_VISIT = 0.15;
const float ALPHA_PER_DAY   = 0.10;
// How many standard deviations away counts as anomalous
const float K_SIGMA = 2.5;
const int NUM_ROOMS = 5;

int bathroomVisitCountToday[NUM_ROOMS] = {0};        // indexed by room, though only BathRoom used here
long bathroomTotalDurationToday[NUM_ROOMS] = {0};    // seconds, summed across today's visits
int bathroomMaxDurationToday[NUM_ROOMS] = {0};       // seconds, longest single visit today
float wakeTimeSecOfDayToday = NAN;                   // set once, when today's wake event fires
int currentDay = -1; // tm_yday, used to detect rollover

// ------------- BedRoom sleep-session tracking (wake-time detection) -------------
// Mirrors extract_features.py: consecutive BedRoom intervals are chained into
// one "merged session" when the gap between them is <= WAKE_MERGE_GAP_SEC
// (the LD2410c briefly loses lock on a very still sleeping person). The wake
// time for a calendar date is the seconds-of-day at the END of that date's
// LONGEST merged session with duration >= WAKE_MIN_DURATION_SEC, attributed
// to the session's END date. The multivariate Isolation Forest check at
// rollover consumes wakeTimeSecOfDayToday, so these MUST stay in sync with
// the --sleep-merge-gap-sec / --min-sleep-duration-sec flags used to train.
const long WAKE_MERGE_GAP_SEC     = 300L;   // == extract_features --sleep-merge-gap-sec
const long WAKE_MIN_DURATION_SEC  = 3L * 3600L; // == extract_features --min-sleep-duration-sec
bool       bedSessionOpen         = false;  // an in-progress merged session
time_t     bedSessionStartEpoch   = 0;
time_t     bedSessionLastEndEpoch = 0;
long       bedLongestDurationSec  = 0;      // longest FINALIZED >=3h session, by END date
time_t     bedLongestEndEpoch     = 0;      // end epoch of that session (candidate wake)
int        bedLongestEndDay       = -1;     // tm_yday of that session's END date

// Learned time-of-day pattern: how many bathroom visits happen in EACH
// hour, today. Each of the 24 bins gets its OWN EWMA baseline (keys
// "bh_00".."bh_23"), so the system learns each person's own usage clock
// (e.g. "this resident typically visits around 6am, 1pm and 10pm") rather
// than assuming any fixed schedule. A visit at an hour that's historically
// near-zero -- or the ABSENCE of a visit at an hour that's normally
// reliable -- both count as "far from the learned pattern".
int bathroomHourCountToday[24] = {0};
bool bathroomHourAlertedToday[24] = {false}; // prevents a double-alert: once immediately, once again at rollover for the same hour


struct RoomState
 {
  bool occupied = false;
  time_t occupiedSinceEpoch = 0;
  time_t lastMsgEpoch = 0;
};

const char* ROOMS[] = {"LivingRoom", "BedRoom", "Toilet-1", "Kitchen", "Hallway"};

RoomState roomState[NUM_ROOMS];
// ---------------- EWMA baseline storage ----------------
typedef struct Baseline 
{
  float mean;
  float var;
  bool initialized;
} Baseline_t;

Preferences prefs;

void InitDataStructure()
{
  int i ;
   for(i=0;i< NUM_ROOMS;i++)
  {
      roomState[i].occupied = false ;

  }
  prefs.begin("anomaly", false) ;
}
// key must be <= 15 chars for Preferences (NVS key length limit)
struct Baseline  loadBaseline(const char* key) 
{
  struct Baseline  b;
  DEBUG_PRINTF("Loading Baseline\n");
  b.mean = prefs.getFloat((String(key) + "m").c_str(), NAN);
  b.var  = prefs.getFloat((String(key) + "v").c_str(), NAN);
  b.initialized = !isnan(b.mean);
  return b;
}

void saveBaseline(const char* key, const struct Baseline  &b) 
{
  DEBUG_PRINTF("Saving Baseline\n");
  prefs.putFloat((String(key) + "m").c_str(), b.mean);
  prefs.putFloat((String(key) + "v").c_str(), b.var);
}

// Returns true if `value` is anomalous vs the CURRENT baseline (checked
// before the baseline is updated with today's value -- so a single bad
// day doesn't immediately mask itself).

bool checkAndUpdateBaseline(const char* key, float value, float alpha, bool highSideOnly = false) 
{
  Baseline b = loadBaseline(key);
  bool anomaly = false;

    DEBUG_PRINTF("checkAndUpdateBaseline\n");
  if (b.initialized) 
  {
    float diff = value - b.mean;
    float sd = sqrt(max(b.var, 1.0f)); // floor variance to avoid div-by-~0 on early data
    if (highSideOnly) 
    {
      anomaly = diff > K_SIGMA * sd;
    } else 
    {
      anomaly = fabs(diff) > K_SIGMA * sd;
    }
    // EWMA update
    float newMean = b.mean + alpha * diff;
    float newVar  = (1 - alpha) * (b.var + alpha * diff * diff);
    b.mean = newMean;
    b.var  = newVar;
  } 
  else 
  {
    // First-ever sample for this metric -- seed the baseline, no anomaly check yet
    b.mean = value;
    b.var  = 0;
    b.initialized = true;
  }
  saveBaseline(key, b);
  return anomaly;
}



int findRoomIndex(const char* room) 
{
  for (int i = 0; i < NUM_ROOMS; i++) 
  {
    if (strcmp(ROOMS[i], room) == 0)
    { 
        return i;
    }
  }
  return -1;
}

/* *****************************************************************************
Function Name : onDataReceived
INPUT PARAMETERS :
   1. MQTT topic of the received message
   2. Payload of the received message
RETURN
   None
FUNCTION
   MQTT ASYCN call back when a message is received 
******************************************************************************/

void onDataReceived(const std::string &topic, const std::string &payload)
{

  char message[200];
  JsonDocument doc;
  bool detected ;
  DEBUG_PRINTF("[MQTT] %s\n",
                topic.c_str(), payload.c_str());
  
  convertEpochToLocalTime(getEpochTime()) ;
  DeserializationError error = deserializeJson(doc, payload);
  if (error) 
  {
    Serial.print("JSON parse failed: ");
    Serial.println(error.c_str());
    return;
  }
  noOfMQTTMessages++ ;

  const char* room   = doc["sensor_room"];
  const char* status = doc["status"];
  bool movingTarget     = doc["Moving Target"];
  bool stationaryTarget = doc["Stationary Target"];
  char event[10] ; // To be incorportated later


  int movingDist     = doc["Moving Target Dist"];
  int stationaryDist = doc["Stationary Target Dist"];
  
    int idx = findRoomIndex(room);
    if (idx < 0) 
    {
      return;
    }
    if (strcmp(status,"KEEP_ALIVE") != 0)
    {
      DEBUG_PRINTF("Room Index = %d , %s %d\n", idx,status,roomState[idx].occupied);
    }
    time_t now = time(nullptr);
    roomState[idx].lastMsgEpoch = now;
    if ((strcmp(status, "DETECTED") == 0) && (roomState[idx].occupied == false))
    {
        Serial.printf("status == DETECTED and occupied == False\n") ;
        roomState[idx].occupied = true;
        roomState[idx].occupiedSinceEpoch = now;
    } 
    else if ((strcmp(status, "NOT_DETECTED") == 0) && (roomState[idx].occupied == true)) 
    {
        Serial.printf("status == NOT_DETECTED and occupied == true\n") ;
        roomState[idx].occupied = false;
        WhenRoomIsEmpty(room, roomState[idx].occupiedSinceEpoch, now);
    }

    else if ((strcmp(status, "KEEP_ALIVE") == 0)) 
    {
         roomState[idx].lastMsgEpoch = time(nullptr);
    }
    else if ((strcmp(status, "DETECTED") == 0) && ((strcmp(event, "FALL") == 0)))
    {
    // ---- Fall alert relay (once fall detection is added back on leaves) ----
        //mqttClient.publish(ALERT_TOPIC, (const char*)payload, true);
    }
}


// ---------------- SD audit log ----------------
void logIntervalToSD(const char* room, time_t startEpoch, time_t endEpoch, int durationSec) 
{
  struct tm *tmNow = localtime(&endEpoch);
  char filename[100];
  snprintf(filename, sizeof(filename), "/intervals/%04d-%02d-%02d.csv",
           tmNow->tm_year + 1900, tmNow->tm_mon + 1, tmNow->tm_mday);
  
  Serial.printf("logIntervalToSD: %s \n",filename);

  File f = SD.open(filename, FILE_APPEND);
  if (f) 
  {
    f.printf("%s,%lu,%lu,%d\n", room, (unsigned long)startEpoch, (unsigned long)endEpoch, durationSec);
    Serial.printf("%s,%lu,%lu,%d\n", room, (unsigned long)startEpoch, (unsigned long)endEpoch, durationSec) ;
    f.close();
  }
}


void WhenRoomIsEmpty(const char* room, time_t startEpoch, time_t endEpoch) 
{
  int timeSpendinRoom = (int)(endEpoch - startEpoch);
  logIntervalToSD(room, startEpoch, endEpoch, timeSpendinRoom);
   DEBUG_PRINTF("WhenRoomIsEmpty\n");

  if (strcmp(room, "Toilet-1") == 0)
  {
    // Real-time per-visit duration check.
    // NOTE: the bathroom sensor on the leaves is named "Toilet-1" (there is
    // no "BathRoom" room); this branch was previously dead because the
    // string never matched.
    bool anomaly = checkAndUpdateBaseline("bath_dur", (float)timeSpendinRoom, ALPHA_PER_VISIT, true);
    if (anomaly == true) 
    {
      Baseline b = loadBaseline("bath_dur");
      publishAlert(room, "visit_duration_sec", timeSpendinRoom, b.mean);
    }
    int idx = findRoomIndex(room);
    if (idx >= 0) 
    {
      bathroomVisitCountToday[idx]++;
      bathroomTotalDurationToday[idx] += timeSpendinRoom;
      if (timeSpendinRoom > bathroomMaxDurationToday[idx]) 
      {
        bathroomMaxDurationToday[idx] = timeSpendinRoom;
      }
    }
    // Time-slot pattern: which hour did this visit START in?
    struct tm *startTm = localtime(&startEpoch);
    int hour = startTm->tm_hour;
    bathroomHourCountToday[hour]++;
    checkHourlyPatternImmediate(hour);
  }

  if (strcmp(room, "BedRoom") == 0)
  {
    // Feeds the multivariate Isolation Forest (wake_time_sec_of_day feature).
    maybeRecordWakeTime(startEpoch, endEpoch, timeSpendinRoom);
    // EXTENSION POINT: bedtime detection -- symmetric to wake time, but
    // using the START of this interval instead of the end, flagged if
    // TOO LATE (high side) or notably early (could indicate illness).
  }

  if (strcmp(room, "LivingRoom") == 0) 
  {
    //maybeRecordWakeTime(startEpoch, endEpoch, durationSec);
    // EXTENSION POINT: bedtime detection -- symmetric to wake time, but
    // using the START of this interval instead of the end, flagged if
    // TOO LATE (high side) or notably early (could indicate illness).
  }
  if (strcmp(room, "Kitchen") == 0) 
  {
    //maybeRecordWakeTime(startEpoch, endEpoch, durationSec);
    // EXTENSION POINT: bedtime detection -- symmetric to wake time, but
    // using the START of this interval instead of the end, flagged if
    // TOO LATE (high side) or notably early (could indicate illness).
  }
 

  if (strcmp(room, "DiningRoom") == 0) 
  {

  }

}


// ---------------- BedRoom wake-time detection ----------------
// Chains consecutive BedRoom intervals into merged sessions (gap <=
// WAKE_MERGE_GAP_SEC) and remembers, per END calendar date, the longest
// merged session that lasted >= WAKE_MIN_DURATION_SEC. This mirrors the
// training-time logic in extract_features.py's compute_wake_times().
void finalizeBedSession()
{
  if (!bedSessionOpen) return;
  long mergedDur = (long)(bedSessionLastEndEpoch - bedSessionStartEpoch);
     DEBUG_PRINTF("finalizeBedSession\n");
  if (mergedDur >= WAKE_MIN_DURATION_SEC)
  {
    // Attribute to the session's END date; keep the LONGEST per date.
    struct tm *t = localtime(&bedSessionLastEndEpoch);
    if (t->tm_yday > bedLongestEndDay || mergedDur > bedLongestDurationSec)
    {
      bedLongestEndDay     = t->tm_yday;
      bedLongestDurationSec = mergedDur;
      bedLongestEndEpoch   = bedSessionLastEndEpoch;
    }
  }
  bedSessionOpen = false;
  bedSessionStartEpoch = 0;
  bedSessionLastEndEpoch = 0;
}

void maybeRecordWakeTime(time_t startEpoch, time_t endEpoch, int durationSec)
{
  (void)durationSec; // the merged-session duration is end - start of the chain
  if (bedSessionOpen && (startEpoch - bedSessionLastEndEpoch) <= WAKE_MERGE_GAP_SEC)
  {
    bedSessionLastEndEpoch = endEpoch; // same merged session, extend it
  }
  else
  {
    finalizeBedSession();              // previous chain (if any) is over
    bedSessionOpen = true;
    bedSessionStartEpoch = startEpoch;
    bedSessionLastEndEpoch = endEpoch;
  }
}

// ---------------- Real-time "ongoing visit too long" check ----------------
// This is the piece that benefits from running locally rather than waiting
// for a batch job: while someone is STILL in the bathroom, compare elapsed
// time so far against the baseline max, not just after they leave.
// --------------------------------------------------------------------------

void checkOngoingToiletVisits()
{
  int idx = findRoomIndex("Toilet-1");
  if (idx < 0 || !roomState[idx].occupied) return;

  time_t now = time(nullptr);
  int elapsed = (int)(now - roomState[idx].occupiedSinceEpoch);
  Baseline b = loadBaseline("bath_dur");
  if (b.initialized  == true)
  {
    float sd = sqrt(max(b.var, 1.0f));
    if (elapsed > b.mean + K_SIGMA * sd && elapsed % 60 == 0)
    {
      // Fires roughly once per minute past threshold, not on every loop tick
      publishAlert("Toilet-1", "ongoing_visit_exceeds_baseline", elapsed, b.mean);
    }
  }
}

void checkDailyRollover() 
{
  time_t now = time(nullptr);
  struct tm *t = localtime(&now);
  if (currentDay == -1) 
  { 
    currentDay = t->tm_yday; return; 
  }
  if (t->tm_yday != currentDay) 
  {
    // New day -- finalize yesterday's day-level metrics
    // (The bathroom sensor room is "Toilet-1", not "BathRoom".)
    int bathroomIdx = findRoomIndex("Toilet-1");
    if (bathroomIdx >= 0)
    {
      float count = bathroomVisitCountToday[bathroomIdx];
      // Both directions matter here: a SPIKE in visits can signal a UTI or
      // similar issue; a DROP can signal reduced fluid/food intake, or
      // reduced mobility keeping them from getting up at all. highSideOnly
      // is intentionally false for this one metric.
      Baseline beforeUpdate = loadBaseline("bath_cnt"); // read mean BEFORE update, just to report direction
      bool anomaly = checkAndUpdateBaseline("bath_cnt", count, ALPHA_PER_DAY, false /*flag both directions*/);
      if (anomaly == true) 
      {
        const char* direction = (beforeUpdate.initialized && count < beforeUpdate.mean)
                                   ? "visit_count_per_day_drop"
                                   : "visit_count_per_day_spike";
        publishAlert("Toilet-1", direction, count, beforeUpdate.mean);
      }

      // Same pattern for the two new daily aggregates -- each metric gets
      // its own EWMA baseline under its own NVS key, exactly like count.
      float totalDur = bathroomTotalDurationToday[bathroomIdx];
      bool durAnomaly = checkAndUpdateBaseline("bath_tot_dur", totalDur, ALPHA_PER_DAY, true);
      if (durAnomaly) 
      {
        Baseline b = loadBaseline("bath_tot_dur");
        publishAlert("BathRoom", "total_duration_per_day_sec", totalDur, b.mean);
      }

      float maxDur = bathroomMaxDurationToday[bathroomIdx];
      bool maxAnomaly = checkAndUpdateBaseline("bath_max_dur", maxDur, ALPHA_PER_DAY, true);
      if (maxAnomaly) 
      {
        Baseline b = loadBaseline("bath_max_dur");
        publishAlert("BathRoom", "max_duration_per_day_sec", maxDur, b.mean);
      }

#ifdef IF_NUM_FEATURES

      float features[IF_NUM_FEATURES] = 
      {
        count,
        totalDur,
        maxDur,
        wakeTimeSecOfDayToday
      };
      if (!isnan(wakeTimeSecOfDayToday)) 
      { // skip days with no qualifying sleep interval
        float score = ifComputeAnomalyScore(features);
        if (score >= IF_ANOMALY_THRESHOLD) 
        {
          publishAlert("House", "multivariate_anomaly", score, IF_ANOMALY_THRESHOLD);
        }
      }
#endif

      bathroomVisitCountToday[bathroomIdx] = 0;
      bathroomTotalDurationToday[bathroomIdx] = 0;
      bathroomMaxDurationToday[bathroomIdx] = 0;
      
      // Hourly time-slot pattern: update each of the 24 learned baselines
      // with today's final count for that hour. Hours already alerted
      // in real time (checkHourlyPatternImmediate) are skipped here to
      // avoid a duplicate alert -- but the baseline still gets updated for
      // every hour regardless, learning continues either way. This pass
      // is also what catches an EXPECTED hour going quiet (e.g. the usual
      // 6am visit not happening) -- nothing during the day would otherwise
      // notice an absence, only a rollover-time comparison can.
      for (int h = 0; h < 24; h++) 
      {
        char key[8];
        snprintf(key, sizeof(key), "bh_%02d", h);
        Baseline beforeHourUpdate = loadBaseline(key);
        bool hourAnomaly = checkAndUpdateBaseline(key, (float)bathroomHourCountToday[h], ALPHA_PER_DAY, false);
        if (hourAnomaly && !bathroomHourAlertedToday[h]) 
        {
          char metric[40];
          snprintf(metric, sizeof(metric), "visit_at_hour_%02d_unusual", h);
          publishAlert("BathRoom", metric, bathroomHourCountToday[h], beforeHourUpdate.mean);
        }
        bathroomHourCountToday[h] = 0;
        bathroomHourAlertedToday[h] = false;
      }
    }
    wakeTimeSecOfDayToday = NAN; // reset for the new day

    // EXTENSION POINT: finalize "did an outing happen today" boolean-style
    // metric here too, e.g.:
    //   float outingHappened = outingOccurredToday ? 1.0 : 0.0;
    //   if (checkAndUpdateBaseline("outing_occ", outingHappened, ALPHA_PER_DAY)) { ... }
    //   outingOccurredToday = false; // reset for the new day

    currentDay = t->tm_yday;
  }
}


/*  ---------------- Hourly time-slot pattern check ----------------
   Called the moment a bathroom visit starts. Compares the running count
   for THIS hour, today, against that hour's learned baseline -- flags
   immediately if a visit happens at an hour that's historically rare for
   this person. Does NOT update the baseline itself (that happens once per
   day at rollover, using the hour's final count) -- this is a peek, so a
   single unusual visit can be caught in real time without waiting for
   midnight, while the actual learning stays a once-daily update.
--------------------------------------------------------------------- */

void checkHourlyPatternImmediate(int hour) 
{
  char key[8];

  if (bathroomHourAlertedToday[hour]) 
  {
    return; // already flagged this hour today, don't repeat
  }

  
  snprintf(key, sizeof(key), "bh_%02d", hour);
  Baseline b = loadBaseline(key);
  if (b.initialized == false) 
  {
     return; // not enough history yet for this hour -- nothing to compare against
  }
  float diff = (float)bathroomHourCountToday[hour] - b.mean;
  float sd = sqrt(max(b.var, 1.0f));
  if (fabs(diff) > K_SIGMA * sd) 
  {
    char metric[40];
    snprintf(metric, sizeof(metric), "visit_at_hour_%02d_unusual", hour);
    publishAlert("BathRoom", metric, bathroomHourCountToday[hour], b.mean);
    bathroomHourAlertedToday[hour] = true;
  }
}


