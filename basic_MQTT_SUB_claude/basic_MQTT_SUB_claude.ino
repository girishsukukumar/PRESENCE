

/* Select Device M5CoreS3 */

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

#define JSON_CONFIG_FILE_NAME "/config.json" 
#define SSID_NAME_LEN 20 
#define SSID_PASSWD_LEN 20
#define NAME_LEN 40
#define MQTT_TOPIC_LENGTH 100
#define PRESENSE_DETECTED 1
#define NO_PRESENCE  0

// CoreS3 microSD SPI pins
#define SD_CS_PIN    4
#define SD_SCK_PIN   36
#define SD_MISO_PIN  35
#define SD_MOSI_PIN  37

// Uses : https://github.com/iavorvel/MyLD2410 
//Search for MyLD2410 library for installing this in arduino IDE
// MQTT Library : https://github.com/cyijun/ESP32MQTTClient

// User defines
#define SERIAL_BAUD_RATE 115200
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






/* *****************************************************************************
Function Name : setup()
INPUT PARAMETERS :
None
   
RETURN
   None
FUNCTION
   1. First function that will be called at power up
   2. Intialized Serial Port
   3. Read the confinguration file config.json from Little FS
   4. Connect to Wifi
   5. Connect to MQTT Broker
   6. Start the Telnet Server for debugging
******************************************************************************/
const char* ntpServer = "pool.ntp.org";
const long  gmtOffset_sec = (5 * 3600) + (1800);     // Five hours and 30 minutes is the difference 
const int   daylightOffset_sec = 0;     // 3600 if your area uses DST, 0 if not
void WhenRoomIsEmpty(const char* room, time_t startEpoch, time_t endEpoch) ;
void checkDailyRollover() ;
void checkOngoingVisits() ;
void InitDataStructure();


typedef struct configData 
{
  char     ssid1[SSID_NAME_LEN]   ;
  char     password1[SSID_PASSWD_LEN]  ;
  char     ssid2[SSID_NAME_LEN] ;
  char     password2[SSID_PASSWD_LEN] ;
  char     ssid3[SSID_NAME_LEN] ;
  char     password3[SSID_PASSWD_LEN] ;
  char     wifiDeviceName[NAME_LEN] ; 
  char     mqttBroker[MQTT_TOPIC_LENGTH] ; 
  char     PUBLISH_TOPIC[MQTT_TOPIC_LENGTH] ;
  char     SUBCRIBING_TOPIC[MQTT_TOPIC_LENGTH];

} configData_t;
configData_t  ConfigData;



const char* timezone =  "IST-5:30";




// Public MQTT test broker (see https://test.mosquitto.org), do not use in production
//const char *server = "mqtt://test.mosquitto.org:1883";

/* ************************************************************************** */
/*    MQTT specific declarations 
****************************************************************************** */

WiFiMulti      wifiMulti; 

ESP32MQTTClient mqttClient; // all params are set later

unsigned long  lastPrint = 0 ;
volatile bool wifiNeedsReconnect = false;
WiFiServer telnetServer(23);
WiFiClient telnetClient;

uint32_t noOfMQTTMessages = 0 ; 



bool ConnectToWifi()
{
  int count ;
  char ssid[40];
  char ipAddr[25];
  String IPaddress;
  DEBUG_PRINTF("ConnectToWifi: begin\n");

  DEBUG_PRINTF("ConnectToWifi: Hostname: %s\n",ConfigData.wifiDeviceName);
  WiFi.onEvent(WiFiStationConnected, ARDUINO_EVENT_WIFI_STA_CONNECTED);
  WiFi.onEvent(WiFiGotIP, ARDUINO_EVENT_WIFI_STA_GOT_IP);
  WiFi.onEvent(WiFiStationDisconnected, ARDUINO_EVENT_WIFI_STA_DISCONNECTED);

  WiFi.setHostname(ConfigData.wifiDeviceName);
  WiFi.setSleep(false);         // <-- disable power save, add this

  wifiMulti.addAP(ConfigData.ssid1, ConfigData.password1);   
  wifiMulti.addAP(ConfigData.ssid2, ConfigData.password2);    
  wifiMulti.addAP(ConfigData.ssid3, ConfigData.password3);    
  count  = 0 ;


  while  (wifiMulti.run()  !=  WL_CONNECTED) 
  { 
    //  Wait  for the Wi-Fi to  connect:  scan  for Wi-Fi networks, and connect to  the strongest of  the networks  above       
    delay(1000);        
    DEBUG_PRINTF("*");    
    count++ ;
    if (count > 40)
    {
       return false ;  
    }
  }   
  delay(5000);
  WiFi.setTxPower(WIFI_POWER_19_5dBm);



  DEBUG_PRINTF("\n");   
  strcpy(ssid,WiFi.SSID().c_str() );
  IPaddress = WiFi.localIP().toString();
  strcpy(ipAddr,IPaddress.c_str());
  DEBUG_PRINTF("Connected to  ");   
  DEBUG_PRINTF("%s\n",ssid);         
  DEBUG_PRINTF("IP  address: %s ",ipAddr);   
  DEBUG_PRINTLN(); 

   // Set mDNS
  if (!MDNS.begin(ConfigData.wifiDeviceName)) 
  {
     DEBUG_PRINTF("Error setting up MDNS responder! \n");
     delay(10000);
  }

  // Add service to MDNS-SD
  MDNS.addService("_http", "_tcp", 80);
  DEBUG_PRINTLN("mDNS responder started. Access your ESP32 at http://" + String(ConfigData.wifiDeviceName) + ".local");

  WiFi.softAPdisconnect (true);   //Disable the Access point mode.
  DEBUG_PRINTF("ConnectToWifi: End\n");

 
  return true ;
}

void publishAlert(const char* room, const char* metric, const int value, const float mean)
{
  // Publishes a JSON anomaly alert over MQTT so the anomaly detection has an
  // actual outward effect. The 4-parameter convention used by all callers in
  // process.ino is: room name, metric/alert-type string, observed value, and
  // the EWMA baseline mean the value was checked against.
  char buf[220];
  time_t now = time(nullptr);
  snprintf(buf, sizeof(buf),
           "{\"sensor_room\":\"%s\",\"alert_type\":\"%s\",\"value\":%d,"
           "\"baseline_mean\":%.1f,\"epoch\":%lu,\"kind\":\"ANOMALY\"}",
           room, metric, value, (double)mean, (unsigned long)now);
  mqttClient.publish(ConfigData.PUBLISH_TOPIC, buf, 0, false);
  DEBUG_PRINTF("[ALERT] %s\n", buf);
}

void ReadConfigValuesFromSPIFF()
{
  File jsonFile ;
  //const size_t capacity = JSON_OBJECT_SIZE(8) + 240;
  JsonDocument  doc;


  jsonFile = SD.open(JSON_CONFIG_FILE_NAME, FILE_READ);
  
  if (!jsonFile)
  {
     DEBUG_PRINTF("Unable to open %s",JSON_CONFIG_FILE_NAME);
     return ;
  }
  
  deserializeJson(doc, jsonFile);
  
  const char* ssid1               = doc["ssid1"] ; // "xxxxxxxxxxxxxxxxxxxx"
  const char* password1           = doc["password1"] ; // "xxxxxxxxxxxxxxxxxxxx"
  const char* ssid2               = doc["ssid2"] ; // "xxxxxxxxxxxxxxxxxxxx"
  const char* password2           = doc["password2"] ; // "xxxxxxxxxxxxxxxxxxxx"
  const char* ssid3               = doc["ssid3"] ; // "xxxxxxxxxxxxxxxxxxxx"
  const char* password3           = doc["password3"] ; // "xxxxxxxxxxxxxxx"  
  const char* devicename          = doc["devicename"] ; // "xxxxxxxxxxxxxx"
  const char* mqttBroker          = doc["mqttbroker"] ; // "xxxxxxxxxxxxxx"
  const char* mqttPublishingtopic = doc["mqttPublishingtopic"] ;
  const char* mqttSubcribingTopic  = doc["mqttSubcribingTopic"] ;
  jsonFile.close();

  DEBUG_PRINTF("-*--Reading for SPIFF starts \n");
  
  strcpy(ConfigData.ssid1,ssid1);
  strcpy(ConfigData.password1,password1);
  DEBUG_PRINTF("%s \n", ssid1);
  DEBUG_PRINTF("%s \n", password1);
  
  strcpy(ConfigData.ssid2,ssid2);
  strcpy(ConfigData.password2,password2);
  DEBUG_PRINTF("%s \n", ssid2);
  DEBUG_PRINTF("%s \n", password2);
  
  strcpy(ConfigData.ssid3,ssid3);
  strcpy(ConfigData.password3,password3);
  DEBUG_PRINTF("%s \n", ssid3);
  DEBUG_PRINTF("%s \n", password3);

  if (strlen(devicename) == 0 )
  {
    DEBUG_PRINTF("devicename field was not read for config.json\n") ;
    return ;
  }
  
  DEBUG_PRINTF(" device name %s \n", devicename);
  strcpy(ConfigData.wifiDeviceName,devicename);
  DEBUG_PRINTF("ReadConfigValues :1 \n") ;
  if (strlen(mqttBroker) == 0 )
  {
    DEBUG_PRINTF("mqttBroked field was not read for config.json\n") ;
    return ;
  }
  DEBUG_PRINTF("%s \n", mqttBroker);
  strcpy(ConfigData.mqttBroker,mqttBroker);

  strcpy(ConfigData.SUBCRIBING_TOPIC, mqttSubcribingTopic);
  strcpy(ConfigData.PUBLISH_TOPIC, mqttPublishingtopic);
  DEBUG_PRINTF("Subscribing topic %s\n",ConfigData.SUBCRIBING_TOPIC );
   DEBUG_PRINTF("Publishing  topic %s\n",ConfigData.PUBLISH_TOPIC );
}

void WiFiStationConnected(WiFiEvent_t event, WiFiEventInfo_t info) 
{
  DEBUG_PRINTF("Connected to AP successfully!\n");
}

void WiFiGotIP(WiFiEvent_t event, WiFiEventInfo_t info)
{
  DEBUG_PRINTF("WiFi connected\n");
  DEBUG_PRINTF("IP address: ");
  DEBUG_PRINTLN(WiFi.localIP());
}

void WiFiStationDisconnected(WiFiEvent_t event, WiFiEventInfo_t info)
{
  DEBUG_PRINTF("Disconnected from WiFi access point\n");
  DEBUG_PRINTF("WiFi lost connection. Reason: ");
  DEBUG_PRINT(info.wifi_sta_disconnected.reason);
  DEBUG_PRINTF("Attempting to reconnect...");
  //WiFi.reconnect();  // Tries to reconnect using stored credentials[web:1]
  // Commented the above code as per recommendataion from Claude and added the
  // following code

   wifiNeedsReconnect = true;  
}
void DisplayConfigValues()
{
   DEBUG_PRINTF("ssid1 %s \n",ConfigData.ssid1);
   DEBUG_PRINTF("Password %s \n", ConfigData.password1);

   DEBUG_PRINTF("ssid2 %s \n",ConfigData.ssid2);
   DEBUG_PRINTF("Password2 %s \n", ConfigData.password2);

   DEBUG_PRINTF("ssid3 %s \n",ConfigData.ssid3);
   DEBUG_PRINTF("Password3 %s \n", ConfigData.password3);

   DEBUG_PRINTF("Device name = %s ", ConfigData.wifiDeviceName);
}

/* *****************************************************************************
Function Name : StartTelnet
INPUT PARAMETERS :
    None
RETURN
   None
FUNCTION
   Starts the Telnet server on the ESP32 
******************************************************************************/
void StartTelnet()
{
  telnetServer.begin();
  telnetServer.setNoDelay(true);
}

/* *****************************************************************************
Function Name : HandleTelnet
INPUT PARAMETERS :
    None
RETURN
   None
FUNCTION
   Handles Telnet specific situations
******************************************************************************/

void HandleTelnet()
{
  if (telnetServer.hasClient())
  {
    if (!telnetClient || !telnetClient.connected())
    {
      if (telnetClient) telnetClient.stop();
         telnetClient = telnetServer.available();
    }
    else
    {
      telnetServer.available().stop(); // reject extra connections
    }
  }
}
/* *****************************************************************************
Function Name : TelnetPrintf
INPUT PARAMETERS :
    None
RETURN
   None
FUNCTION
    Writes to Telnet port so that we can debug using Telnet session 
******************************************************************************/
void TelnetPrintf(const char* fmt, ...)
{
  char buf[200];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);

  if (telnetClient && telnetClient.connected())
  {
    telnetClient.print(buf);      // mirror to telnet if connected
  }
}



/* *****************************************************************************
Function Name : onMqttConnect
INPUT PARAMETERS :
   1. Client Handle
   
RETURN
   None
FUNCTION
   1. MQTT ASYCN call back when MQTT Client connected to BROKER
   2. Installed the ASYNC call back that need to be called when message is recevied
   3. Publishes a messaage that the client is connected
******************************************************************************/

void onMqttConnect(esp_mqtt_client_handle_t client)
{
 char message[100];
 long rssi = WiFi.RSSI();
  DEBUG_PRINTF("[MQTT] Connected to broker\n");
 
  // Subscribe and point it at the named function above.
  // (You can pass a function pointer directly — no lambda needed.)
  mqttClient.subscribe(ConfigData.SUBCRIBING_TOPIC, onDataReceived);
 
  // You can subscribe to more topics, each with its own handler:
  // mqttClient.subscribe("esp32/other/topic", onDataReceived);
 
  // Publish a "hello" message right after connecting
  snprintf(message, sizeof(message),
               "{\"sensor_room\":\"%s\", \"status\" : \"ON_LINE\", \"Wifi\" : \"%ld\" }", ConfigData.wifiDeviceName,rssi);
  //sprintf(message,"{  \"Message\" : \"%s connected\"  }",ConfigData.wifiDeviceName) ;
  mqttClient.publish(ConfigData.PUBLISH_TOPIC , message, 0, false);

}
/* *****************************************************************************
Function Name : handleMQTT
INPUT PARAMETERS :
void *handler_args, 
esp_event_base_t base
int32_t event_id, 
void *event_dat
   
RETURN
   None
FUNCTION
 TBD
******************************************************************************/

void handleMQTT(void *handler_args, esp_event_base_t base,
                int32_t event_id, void *event_data)
{
  (void)base;
  (void)event_id;
  auto *client = static_cast<ESP32MQTTClient *>(handler_args);
  auto *event  = static_cast<esp_mqtt_event_handle_t>(event_data);
  client->onEventCallback(event);
}

unsigned long getEpochTime()
 {
  time_t now;
  struct tm timeinfo;
  if (getLocalTime(&timeinfo) == false) 
  {
    DEBUG_PRINTF("Unable to get Local time \n") ;
    return 0; // Return 0 if time failed to sync
  }
  time(&now);
  return now;
}

void convertEpochToLocalTime(unsigned long epochTime) 
{
  time_t rawTime = (time_t)epochTime;
  
  // CRITICAL CHANGE: Use localtime() instead of gmtime()
  struct tm *ti = localtime(&rawTime); 
  
  int year   = ti->tm_year + 1900; 
  int month  = ti->tm_mon + 1;    
  int day    = ti->tm_mday;         
  int hour   = ti->tm_hour;        
  int minute = ti->tm_min;       
  int second = ti->tm_sec;       

  Serial.printf("Local Time: %04d-%02d-%02d %02d:%02d:%02d\n", 
                year, month, day, hour, minute, second);

}


uint32_t lastOngoingCheckMs = 0;  // Used only by loop()
void setup() 
{
  
  char message[100];
  File fp;
  unsigned long epochTime ; 


  auto cfg = M5.config();

  M5.begin(cfg);

  M5.Display.setRotation(1);
  M5.Display.fillScreen(BLACK);
  M5.Display.setTextColor(GREEN);
  M5.Display.setTextSize(2);
  M5.Display.setCursor(20, 30);
  M5.Display.setTextScroll(true);
    
    // 2. Define the scrolling area (X, Y, Width, Height)
    // This maps the scrolling box to fill the entire dimensions of your M5 display
  M5.Display.setScrollRect(0, 0, M5.Display.width(), M5.Display.height());

  Serial.begin(SERIAL_BAUD_RATE);
  delay(2000);
  DEBUG_PRINTF("Serial port started\n") ;

    // Initialize the SPI bus for the CoreS3 SD card
  SPI.begin(SD_SCK_PIN, SD_MISO_PIN, SD_MOSI_PIN, SD_CS_PIN);

  // Initialize SD card at 25 MHz
  if (!SD.begin(SD_CS_PIN, SPI, 25000000)) 
  {
    Serial.println("SD card initialization failed");

    M5.Display.setTextColor(RED);
    M5.Display.println("SD init failed");
    M5.Display.println("Check card and wiring");

    while (true) 
    {
      M5.update();
      delay(1000);
    }
  }
 M5.Display.printf("SD card initialized");
  Serial.println("SD card initialized");

  uint8_t cardType = SD.cardType();

  Serial.print("Card type: ");

  if (cardType == CARD_NONE) 
  {
    Serial.println("No SD card");
  } 
  else if (cardType == CARD_MMC) 
  {
    Serial.println("MMC");
  } 
  else if (cardType == CARD_SD) 
  {
    Serial.println("SDSC");
  } 
  else if (cardType == CARD_SDHC) 
  {
    Serial.println("SDHC");
  } 
  else 
  {
    Serial.println("UNKNOWN");
  }

  Serial.print("Card size: ");
  Serial.print(SD.cardSize() / (1024 * 1024));
  Serial.println(" MB");

  ReadConfigValuesFromSPIFF();
  DisplayConfigValues();
  ConnectToWifi(); 
  delay(2000);
  DEBUG_PRINTF("Done!\n");

  configTime(gmtOffset_sec, 0, ntpServer);

  // Reduce it, e.g. to 2 seconds
  M5.Display.printf("WiFi Connected");
  DEBUG_PRINTF("1\n");
  mqttClient.enableDebuggingMessages();
  DEBUG_PRINTF("2\n");
  mqttClient.setURI(ConfigData.mqttBroker);
  DEBUG_PRINTF("3\n");
  mqttClient.setMqttClientName(ConfigData.wifiDeviceName);
  DEBUG_PRINTF("4\n");
   snprintf(message, sizeof(message),
               "{ \"sensor_room\":\"%s\", \"status\" : \"OFF_LINE\" }" ,ConfigData.wifiDeviceName);

  mqttClient.enableLastWillMessage(ConfigData.PUBLISH_TOPIC , message, true);
  DEBUG_PRINTF("5\n");
  mqttClient.setKeepAlive(30);
 
  mqttClient.loopStart(); // returns immediately; runs in background task
  InitDataStructure(); 
  StartTelnet() ;
  epochTime  = getEpochTime();
  convertEpochToLocalTime(epochTime) ;

  DEBUG_PRINTF("Setup Completed \n");
  M5.Display.printf("Setup Completed\n");
}
/* *****************************************************************************
Function Name : loop
INPUT PARAMETERS :
   None
RETURN
   None
FUNCTION
 Calls every 200 milliseconds
******************************************************************************/
void loop()
{

  if (millis() - lastOngoingCheckMs > 10000)
  { // every 10s is plenty for this check
    checkOngoingToiletVisits();
    checkDailyRollover();
    lastOngoingCheckMs = millis();
    M5.Display.printf("MQTT msg = %d\n", noOfMQTTMessages);

  }
  M5.update();
}
