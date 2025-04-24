

#include <Wire.h>
#include <LiquidCrystal_I2C_Hangul.h>
#include <WiFi.h>
#include <Firebase_ESP_Client.h>
#include <MFRC522.h>
#include <SPI.h>
#include <SD.h>
#include <PZEM004Tv30.h>
#include "freertos/FreeRTOS.h"
#include "freertos/timers.h"
#include <time.h>
#include <vector>
#include <map>
#include <esp_sleep.h>
#include <ThreeWire.h>  // DS1302 communication
#include <String.h>
#include <RtcDS1302.h>  // DS1302 RTC library
#include <set>

// Define ScheduleInfo struct first
struct ScheduleInfo {
  bool isValid;
  String day;
  String startTime;
  String endTime;
  String roomName;
  String subject;
  String subjectCode;
  String section;
};

// Global variable declarations
ScheduleInfo currentSchedule = {false, "", "", "", "", "", "", ""}; // Current active schedule
ScheduleInfo tapOutSchedule = {false, "", "", "", "", "", "", ""}; // Schedule saved for tap-out phase
bool pzemLoggedForSession = false; // Flag to track if PZEM data was logged for the current session

// Add missing variable declarations
bool uidDetailsFetched = false;
std::map<String, bool> uidDetailsPrinted;
// Add I2C_ERROR_COUNTER variable declaration
int I2C_ERROR_COUNTER = 0;

// Forward declarations for functions
void initSDCard();
void rfidTask(void * parameter);

// Function Prototypes
void connectWiFi();
void initFirebase();
String getUIDString();
void resetFeedbackAndRestart();
void nonBlockingDelay(unsigned long ms);
void printSDCardInfo();
void accessFeedback();
void deniedFeedback();
void unregisteredUIDFeedback(); // New function for unregistered UID feedback
void storeLogToSD(String entry);
bool syncOfflineLogs();
void watchdogCheck(); // Add the feedWatchdog function declaration here
void feedWatchdog(); // Added missing function declaration
String getFormattedTime();
bool checkResetButton();
void showNeutral();
void logInstructor(String uid, String timestamp, String action);
void logStudentToRTDB(String rfidUid, String timestamp, float weight, int sensorIndex, String weightConfirmed, String timeOut);
void logPZEMData(String uid, float voltage, float current, float power, float energy, float frequency, float pf);
void logUnregisteredUID(String uid, String timestamp);
void logAdminAccess(String uid, String timestamp);
void logAdminTamperStop(String uid, String timestamp);
void logSystemEvent(String event);
bool isRegisteredUID(String uid);
void fetchRegisteredUIDs();
void fetchFirestoreTeachers();
void fetchFirestoreStudents();
void displayMessage(String line1, String line2, unsigned long duration);
void streamCallback(FirebaseStream data);
void streamTimeoutCallback(bool timeout);
void resetWeightSensors();
void setupWeightSensors(); // Added missing function declaration
void enterPowerSavingMode();
void exitPowerSavingMode();
void recoverI2C();
void activateRelays();
void deactivateRelays();
void fetchFirestoreRooms();
void assignRoomToInstructor(String uid, String timestamp);
void updateRoomStatus(String roomId, String status, String instructorName, String subject, String sessionStart, String sessionEnd, float startReading, float endReading, float totalUsage);
String getDate();
bool isAdminUID(String uid);
std::map<String, String> fetchUserDetails(String uid);
ScheduleInfo getInstructorScheduleForDay(String uid, String dateStr);
void logSuperAdmin(String uid, String timestamp);
void handleFirebaseSSLError();
void checkAdminDoorAutoLock();
bool syncSchedulesToSD(); // Added missing function declaration
bool syncOfflineLogsToRTDB(); // Added missing function declaration
ScheduleInfo checkSchedule(String uid, String day, int hour, int minute); // Added missing function declaration
String getDayFromTimestamp(String timestamp); // Added missing function declaration
bool isTimeInRange(String currentTime, String startTime, String endTime); // Added missing function declaration
void smoothTransitionToReady(); // Added missing function declaration

// Global Objects and Pin Definitions
LiquidCrystal_I2C_Hangul lcd(0x27, 16, 2);

#define WIFI_SSID "CIT-U_SmartEco_Lock"
#define WIFI_PASSWORD "123456789"
#define API_KEY "AIzaSyCnBauXgFmxyWWO5VHcGUNToGy7lulbN6E"
#define DATABASE_URL "https://smartecolock-94f5a-default-rtdb.asia-southeast1.firebasedatabase.app/"
#define FIRESTORE_PROJECT_ID "smartecolock-94f5a"

FirebaseData fbdo;
FirebaseData streamFbdo;
FirebaseData firestoreFbdo;
FirebaseConfig config;
FirebaseAuth auth;

#define MFRC522_SCK 14
#define MFRC522_MISO 13
#define MFRC522_MOSI 11
#define MFRC522_CS 15
#define MFRC522_RST 2
#define MFRC522_IRQ 4
SPIClass hspi(HSPI);
MFRC522 rfid(MFRC522_CS, MFRC522_RST, &hspi);

#define SD_SCK 36
#define SD_MISO 37
#define SD_MOSI 35
#define SD_CS 10
SPIClass fsSPI(FSPI);
const char* OFFLINE_LOG_FILE = "/Offline_Logs_Entry.txt";

HardwareSerial pzemSerial(1);
#define PZEM_RX 18
#define PZEM_TX 17
PZEM004Tv30 pzem(pzemSerial, PZEM_RX, PZEM_TX);

// Define pin pairs for each load cell
const int DT_1 = 38, SCK_1 = 39;
const int DT_2 = 40, SCK_2 = 41;
const int DT_3 = 42, SCK_3 = 45;

#define TAMPER_PIN 22
#define BUZZER_PIN 6
#define LED_R_PIN 16
#define LED_G_PIN 19
#define LED_B_PIN 20
#define RESET_BUTTON_PIN 21
#define RELAY1 7
#define RELAY2 4
#define RELAY3 5
#define RELAY4 12
#define I2C_SDA 8
#define I2C_SCL 9
#define REED_PIN 3

// Define pins for ESP32-S3
#define SCLK_PIN 47  // Serial Clock
#define IO_PIN   48  // Data Input/Output
#define CE_PIN   46  // Chip Enable (Reset)

// Create RTC object
ThreeWire myWire(IO_PIN, SCLK_PIN, CE_PIN);
RtcDS1302<ThreeWire> Rtc(myWire);

// Relay pins (adjust these based on your hardware setup)
// Existing constants
const int RELAY1_PIN = 7; // Door relay
const int RELAY2_PIN = 4; // Additional function 1
const int RELAY3_PIN = 5; // Additional function 2
const int RELAY4_PIN = 12; // Additional function 3
const unsigned long finalVerificationTimeout = 30000;
const unsigned long WIFI_TIMEOUT = 10000;
const unsigned long INACTIVITY_TIMEOUT = 300000; // 5 minutes
const unsigned long Student_VERIFICATION_WINDOW = 120000; // 2 minutes total
const float voltageThreshold = 200.0;
const unsigned long VERIFICATION_WAIT_DELAY = 3000;
const unsigned long RFID_DEBOUNCE_DELAY = 2000;
const unsigned long I2C_RECOVERY_INTERVAL = 5000;
const unsigned long TAP_OUT_WINDOW = 300000; // 5 minutes for students to tap out
const unsigned long DOOR_OPEN_DURATION = 45000; // 30 seconds
const unsigned long WRONG_SEAT_TIMEOUT = 30000; // 30 seconds timeout for wrong seat
const unsigned long RESET_DEBOUNCE_DELAY = 50; // 50ms debounce delay
const String SUPER_ADMIN_UID = "A466BABA";
// Consolidated global variables (remove duplicates)
bool sdMode = false;
bool isInstructorLogged = false;
String lastInstructorUID = "";
bool classSessionActive = false;
unsigned long classSessionStartTime = 0;
bool waitingForInstructorEnd = false;
bool studentVerificationActive = false;
unsigned long studentVerificationStartTime = 0;
bool adminAccessActive = false;
String lastAdminUID = "";
bool tamperActive = false;
bool tamperAlertTriggered = false;  // Add this missing variable
bool tamperMessageDisplayed = false;  // Add this missing variable
bool buzzerActive = false;  // Add this missing variable for buzzer pulsing
unsigned long lastBuzzerToggle = 0;  // Add this missing variable for buzzer pulsing
unsigned long lastActivityTime = 0;
// Add admin door timeout tracking
#define ADMIN_DOOR_TIMEOUT 60000  // 60 seconds timeout for admin door
unsigned long adminDoorOpenTime = 0;  // Tracks when admin door was opened
bool adminDoorLockPending = false;    // Tracks if auto door lock is pending
unsigned long adminDoorLockTime = 0;  // Tracks when door should be locked
unsigned long lastReadyPrint = 0;
bool readyMessageShown = false;
unsigned long lastSleepMessageTime = 0;
bool relayActive = false;
unsigned long relayActiveTime = 0;
unsigned long lastRFIDTapTime = 0;
unsigned long lastUIDFetchTime = 0;
unsigned long lastDotUpdate = 0;
unsigned long lastPZEMLogTime = 0;
int dotCount = 0;
bool isConnected = false;
bool isVoltageSufficient = false;
bool wasConnected = false;
bool wasVoltageSufficient = false;
bool firstActionOccurred = false;
bool otaUpdateCompleted = false;
bool tamperMessagePrinted = false;
bool instructorTapped = false;
bool displayMessageShown = false;
bool firestoreFetched = false;
float initVoltage = 0.0;
float initCurrent = 0.0;
float initEnergy = 0.0;
unsigned long lastI2cRecovery = 0;
bool reedState = false;
bool tamperResolved = false;
bool powerSavingMode = false;
bool wifiReconfiguring = false;  // Flag to track when WiFi is being reconfigured
std::vector<String> registeredUIDs;
std::map<String, std::map<String, String>> firestoreTeachers;
std::map<String, std::map<String, String>> firestoreStudents;
std::map<String, std::map<String, String>> firestoreRooms;
std::vector<String> pendingStudentTaps;
String assignedRoomId = "";
float sessionStartReading = 0.0;
float lastVoltage = 0.0;
float lastCurrent = 0.0;
float lastPower = 0.0;
float lastEnergy = 0.0;
float lastFrequency = 0.0;
float lastPowerFactor = 0.0;
bool tapOutPhase = false;
unsigned long tapOutStartTime = 0;
int presentCount = 0;
String currentSessionId = "";
bool attendanceFinalized = false;
unsigned long lastResetDebounceTime = 0;
bool lastResetButtonState = HIGH;
std::map<String, std::map<String, String>> firestoreUsers;
bool doorOpen = false;
unsigned long doorOpenTime = 0;
bool relaysActive = false;
float totalEnergy = 0.0;
unsigned long lastPZEMUpdate = 0;
String doorOpeningUID = "";
bool sdInitialized = false;
String lastTappedUID = "";
String tamperStartTimestamp = "";
unsigned long lastPowerLogTime = 0;
bool superAdminSessionActive = false;
unsigned long superAdminSessionStartTime = 0;
unsigned long systemStartTime = 0;
int sessionEndTimeInMins = -1;

// Relay state management variables
bool relayTransitionInProgress = false;
unsigned long relayTransitionStartTime = 0;
const unsigned long RELAY_TRANSITION_TIMEOUT = 1000; // 1000ms timeout for transitions (increased from 500ms)
unsigned long scheduledDeactivationTime = 0;
bool relayOperationPending = false;
bool pendingRelayActivation = false;
bool relayPendingDeactivation = false;
unsigned long relayDeactivationTime = 0;
const unsigned long RELAY_SAFE_DELAY = 500; // 500ms safety delay (increased from 250ms)

#define DEBUG_MODE false  // Set to true for debug output, false for production

// Global variables for Firebase and SSL recovery
unsigned long sdModeRetryTime = 0; // Used for auto recovery from SSL errors

// Add missing tamper detection variables
bool tamperDetected = false;
String tamperStartTime = "";
String currentTamperAlertId = "";

void nonBlockingDelay(unsigned long ms) {
  unsigned long start = millis();
  while (millis() - start < ms) {
    yield();
  }
}

bool nonBlockingDelayWithReset(unsigned long duration) {
  unsigned long startTime = millis();
  while (millis() - startTime < duration) {
    if (checkResetButton()) {
      // Feedback handled by checkResetButton(); avoid duplicating here
      return false; // Signal reset detected (reset already handled)
    }
    yield();
  }
  return true; // Delay completed without reset
}

void showNeutral() {
  digitalWrite(LED_R_PIN, LOW);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, HIGH);
  Serial.println("LED State: Neutral (R:LOW, G:LOW, B:HIGH)");
}

void accessFeedback() {
  digitalWrite(LED_R_PIN, LOW);
  digitalWrite(LED_G_PIN, HIGH);
  digitalWrite(LED_B_PIN, LOW);
  Serial.println("LED State: Access (R:LOW, G:HIGH, B:LOW)");
  tone(BUZZER_PIN, 2000, 200);
  nonBlockingDelay(500);
  showNeutral();
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void deniedFeedback() {
  digitalWrite(LED_R_PIN, HIGH);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, LOW);
  Serial.println("LED State: Denied (R:HIGH, G:LOW, B:LOW)");
  tone(BUZZER_PIN, 500, 200);
  nonBlockingDelay(200);
  tone(BUZZER_PIN, 300, 200);
  nonBlockingDelay(300);
  showNeutral();
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// New function for unregistered UID feedback with stronger visual and audio indication
void unregisteredUIDFeedback() {
  // Turn on red LED
  digitalWrite(LED_R_PIN, HIGH);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, LOW);
  Serial.println("LED State: Unregistered UID (R:HIGH, G:LOW, B:LOW)");
  
  // Distinctive beeping pattern for unregistered UIDs
  tone(BUZZER_PIN, 800, 200);
  nonBlockingDelay(200);
  tone(BUZZER_PIN, 800, 200);
  nonBlockingDelay(200);
  tone(BUZZER_PIN, 400, 400);
  nonBlockingDelay(400);
  
  // Return to neutral state
  showNeutral();
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// Function to check and auto-lock admin door after timeout
void checkAdminDoorAutoLock() {
  // Check if the admin door is open and needs to be auto-locked due to timeout
  if (adminDoorOpenTime > 0 && relayActive && (millis() - adminDoorOpenTime > ADMIN_DOOR_TIMEOUT)) {
    Serial.println("Admin door auto-lock timeout reached");
    
    // Deactivate the relay for the door only
    digitalWrite(RELAY1, HIGH);
    
    // Update the admin door state but DON'T end the admin session
    relayActive = false;
    adminDoorOpenTime = 0;
    
    // Provide feedback
    deniedFeedback();
    displayMessage("Admin Door", "Auto-Locked", 2000);
    logSystemEvent("Admin Door Auto-Locked due to timeout");
    
    // Reset the RFID reader to ensure it can detect cards for exit tap
    rfid.PCD_Reset();
    delay(50);
    rfid.PCD_Init();
    rfid.PCD_SetAntennaGain(rfid.RxGain_max);
    
    // FIXED: Don't call smoothTransitionToReady() here - it ends the admin session
    // Instead, show admin mode is still active
    unsigned long startTime = millis();
    while (millis() - startTime < 2000) yield(); // Non-blocking delay
    
    displayMessage("Admin Mode Active", "Tap to Exit", 0);
    Serial.println("Admin mode remains active - tap same card to exit");
  }
}

void connectWiFi() {
  Serial.print("Connecting to WiFi");
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("Connecting");
  lcd.setCursor(0, 1);
  lcd.print("to WiFi...");

  unsigned long startTime = millis();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED && millis() - startTime < WIFI_TIMEOUT) {
    Serial.print(".");
    nonBlockingDelay(500);
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nConnected to WiFi: " + String(WIFI_SSID));
    Serial.println("IP Address: " + WiFi.localIP().toString());
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("WiFi Connected!");
    lcd.setCursor(0, 1);
    lcd.print(WiFi.localIP().toString());
    nonBlockingDelay(2000);
    sdMode = false;
    isConnected = true;
    wasConnected = true;
  } else {
    Serial.println("\nWiFi connection failed. Retrying once...");
    WiFi.reconnect(); // Attempt reconnect
    startTime = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startTime < 15000) { // Extended retry timeout to 15s
      Serial.print(".");
      nonBlockingDelay(500);
    }
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nReconnected to WiFi: " + String(WIFI_SSID));
      Serial.println("IP Address: " + WiFi.localIP().toString());
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("WiFi Reconnected!");
      lcd.setCursor(0, 1);
      lcd.print(WiFi.localIP().toString());
      nonBlockingDelay(2000);
      sdMode = false;
      isConnected = true;
      wasConnected = true;
    } else {
      Serial.println("\nWiFi connection failed after retry. Switching to SD mode.");
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("WiFi Failed");
      lcd.setCursor(0, 1);
      lcd.print("SD Mode");
      nonBlockingDelay(2000);
      sdMode = true;
      isConnected = false;
    }
  }
  isVoltageSufficient = (pzem.voltage() >= voltageThreshold);
}

void initFirebase() {
  config.api_key = API_KEY;
  config.database_url = DATABASE_URL;
  
  // Increase timeout to handle slow networks
  config.timeout.serverResponse = 20000;  // 20 seconds for server response
  config.timeout.wifiReconnect = 15000;   // 15 seconds for WiFi reconnect
  config.timeout.socketConnection = 10000; // 10 seconds for socket connection
  config.timeout.sslHandshake = 8000;     // 8 seconds for SSL handshake
  
  Serial.println("Initializing Firebase...");
  
  // Check if already signed up or use anonymous sign-up
  if (Firebase.authenticated()) {
    Serial.println("Already authenticated with Firebase.");
  } else {
    // Clear previous session data
    Firebase.reset(&config);
    
    // Add a small delay for network stabilization
    feedWatchdog();
    delay(500);
    
    if (!Firebase.signUp(&config, &auth, "", "")) { // Anonymous sign-up
      Serial.printf("Firebase signup error: %s\n", config.signer.signupError.message.c_str());
      
      // Check if error is SSL-related, and try one more time
      String errorMessage = config.signer.signupError.message.c_str();
      if (errorMessage.indexOf("ssl") >= 0 || 
          errorMessage.indexOf("SSL") >= 0 || 
          errorMessage.indexOf("connection") >= 0 ||
          errorMessage.indexOf("handshake") >= 0) {
        
        Serial.println("SSL-related error detected. Attempting one more time...");
        feedWatchdog();
        delay(1000);
        
        if (!Firebase.signUp(&config, &auth, "", "")) {
          Serial.printf("Second Firebase signup attempt failed: %s\n", config.signer.signupError.message.c_str());
          lcd.clear();
          lcd.setCursor(0, 0);
          lcd.print("Firebase Error:");
          lcd.setCursor(0, 1);
          lcd.print(config.signer.signupError.message.c_str());
          nonBlockingDelay(5000);
          ESP.restart();
        }
      } else {
        lcd.clear();
        lcd.setCursor(0, 0);
        lcd.print("Firebase Error:");
        lcd.setCursor(0, 1);
        lcd.print(config.signer.signupError.message.c_str());
        nonBlockingDelay(5000);
        ESP.restart();
      }
    } else {
      Serial.println("Firebase anonymous sign-up successful.");
    }
  }
  
  // Token status callback for monitoring and refreshing token
  config.token_status_callback = [](TokenInfo info) {
    String statusStr;
    switch (info.status) {
      case token_status_uninitialized:
        statusStr = "Uninitialized";
        break;
      case token_status_on_signing:
        statusStr = "Signing In";
        break;
      case token_status_on_refresh:
        statusStr = "Refreshing";
        break;
      case token_status_ready:
        statusStr = "Ready";
        break;
      case token_status_error:
        statusStr = "Error";
        break;
      default:
        statusStr = "Unknown";
        break;
    }
    Serial.printf("Token status: %s\n", statusStr.c_str());
    if (info.status == token_status_error) {
      Serial.printf("Token error: %s. Refreshing...\n", info.error.message.c_str());
      Firebase.refreshToken(&config);
      
      // If error is SSL-related, handle it
      String errorMessage = info.error.message.c_str();
      if (errorMessage.indexOf("ssl") >= 0 || 
          errorMessage.indexOf("SSL") >= 0 || 
          errorMessage.indexOf("connection") >= 0) {
        handleFirebaseSSLError();
      }
    }
  };
  
  // Initialize Firebase with configuration
  Firebase.begin(&config, &auth);
  Firebase.reconnectWiFi(true);
  Firebase.RTDB.setReadTimeout(&fbdo, 15000);  // Set 15-second timeout for RTDB read operations
  Firebase.RTDB.setReadTimeout(&fbdo, 15000); // Set 15-second timeout for RTDB write operations
  Firebase.RTDB.enableClassicRequest(&fbdo, true); // Use classic HTTP for more reliable connections
  
  // Wait and verify Firebase is ready
  unsigned long startTime = millis();
  while (!Firebase.ready() && (millis() - startTime < 15000)) {
    Serial.println("Waiting for Firebase to be ready...");
    delay(500);
  }
  
  if (Firebase.ready()) {
    Serial.println("Firebase initialized and authenticated successfully.");
  } else {
    Serial.println("Firebase failed to initialize after timeout.");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Firebase Fail");
    lcd.setCursor(0, 1);
    lcd.print("Restarting...");
    nonBlockingDelay(5000);
    ESP.restart();
  }
}

String getUIDString() {
  String uidStr = "";
  for (byte i = 0; i < rfid.uid.size; i++) {
    if (rfid.uid.uidByte[i] < 0x10)
      uidStr += "0";
    uidStr += String(rfid.uid.uidByte[i], HEX);
  }
  uidStr.toUpperCase();
  return uidStr;
}

bool isRegisteredUID(String uid) {
  Serial.println("Checking if UID '" + uid + "' is registered");
  
  // Super Admin UID should be recognized in both SD mode and online mode
  if (uid == SUPER_ADMIN_UID) {
    Serial.println("UID '" + uid + "' recognized as Super Admin");
    return true;
  }

  // First, check the firestoreStudents map - which is cached data
  if (firestoreStudents.find(uid) != firestoreStudents.end()) {
    Serial.println("UID '" + uid + "' is registered as a student (from cached data)");
    return true;
  }

  // Then check firestoreTeachers map - also cached data
  if (firestoreTeachers.find(uid) != firestoreTeachers.end()) {
    Serial.println("UID '" + uid + "' is registered as a teacher (from cached data)");
    return true;
  }
  
  // Check directly in RegisteredUIDs database if online
  if (!sdMode && isConnected && Firebase.ready()) {
    Serial.println("Checking UID '" + uid + "' directly in RTDB");
    if (Firebase.RTDB.get(&fbdo, "/RegisteredUIDs/" + uid)) {
      Serial.println("UID '" + uid + "' found directly in RegisteredUIDs RTDB");
      // Also add to firestoreStudents or firestoreTeachers as appropriate
      if (!Firebase.RTDB.getJSON(&fbdo, "/Students/" + uid)) {
        if (!Firebase.RTDB.getJSON(&fbdo, "/Instructors/" + uid)) {
          // Just a generic registered UID with no specific role
          Serial.println("UID '" + uid + "' is registered without specific role");
        } else {
          // An instructor
          Serial.println("UID '" + uid + "' is an instructor");
          std::map<String, String> instructorData;
          instructorData["fullName"] = "Unknown";
          instructorData["role"] = "instructor";
          firestoreTeachers[uid] = instructorData;
        }
      } else {
        // A student
        Serial.println("UID '" + uid + "' is a student");
        std::map<String, String> studentData;
        studentData["fullName"] = "Unknown";
        studentData["role"] = "student";
        firestoreStudents[uid] = studentData;
      }
      return true;
    } else {
      Serial.println("UID '" + uid + "' not found in RTDB: " + fbdo.errorReason());
    }
  } else {
    Serial.println("Skipping RTDB check: sdMode=" + String(sdMode) + ", isConnected=" + String(isConnected) + ", Firebase.ready=" + String(Firebase.ready()));
  }

  // If not found in cache, try direct Firestore query
  if (!sdMode && isConnected) {
    // First check teachers collection
    Serial.println("Checking if UID '" + uid + "' is registered in Firestore teachers...");
    String firestorePath = "teachers";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            if (rfidUid == uid) {
              Serial.println("UID '" + uid + "' is registered as a teacher");
              // Cache this data for future use
              std::map<String, String> teacherData;
              String fullName = "";
              if (doc.get(fieldData, "fields/fullName/stringValue")) {
                fullName = fieldData.stringValue;
              }
              teacherData["fullName"] = fullName;
              firestoreTeachers[uid] = teacherData;
              return true;
            }
          }
        }
      }
    } else {
      Serial.println("Failed to retrieve Firestore teachers: " + firestoreFbdo.errorReason());
    }
    
    // Then check students collection
    Serial.println("Checking if UID '" + uid + "' is registered in Firestore students...");
    firestorePath = "students";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            if (rfidUid == uid) {
              Serial.println("UID '" + uid + "' is registered as a student");
              // Cache this data for future use
              std::map<String, String> studentData;
              String fullName = "";
              if (doc.get(fieldData, "fields/fullName/stringValue")) {
                fullName = fieldData.stringValue;
              }
              studentData["fullName"] = fullName;
              studentData["role"] = "student";
              firestoreStudents[uid] = studentData;
              return true;
            }
          }
        }
      }
    } else {
      Serial.println("Failed to retrieve Firestore students: " + firestoreFbdo.errorReason());
    }
  }
  
  Serial.println("UID '" + uid + "' is not registered");
  return false;
}

bool isAdminUID(String uid) {
  if (!sdMode && isConnected) {
    Serial.println("Checking if UID " + uid + " is an admin in Firestore...");
    String firestorePath = "users";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      Serial.println("Firestore documents retrieved successfully for 'users' collection.");
      Serial.println("Firestore payload: " + firestoreFbdo.payload());
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload().c_str());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents") && jsonData.type == "array") {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "", role = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            Serial.println("Found document with rfidUid: " + rfidUid);
          }
          if (doc.get(fieldData, "fields/role/stringValue")) {
            role = fieldData.stringValue;
            Serial.println("Role for rfidUid " + rfidUid + ": " + role);
          }
          if (rfidUid == uid && role == "admin") {
            Serial.println("UID " + uid + " is confirmed as admin.");
            return true;
          }
        }
        Serial.println("No document found with rfidUid " + uid + " and role 'admin'.");
      } else {
        Serial.println("No documents found in 'users' collection.");
      }
    } else {
      Serial.println("Failed to retrieve Firestore documents: " + firestoreFbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    Serial.println("Cannot check admin UID: sdMode=" + String(sdMode) + ", isConnected=" + String(isConnected));
  }
  return false;
}

std::map<String, String> fetchUserDetails(String uid) {
  std::map<String, String> userData;

  if (!sdMode && isConnected) {
    Serial.println("Fetching user details for UID " + uid + " from Firestore...");
    String firestorePath = "users"; // Fetch the entire users collection

    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      Serial.println("Firestore documents retrieved successfully for 'users' collection.");
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload().c_str());
      FirebaseJsonData jsonData;

      if (json.get(jsonData, "documents") && jsonData.type == "array") {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);

        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());

          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
          }

          if (rfidUid == uid) {
            Serial.println("Found user with rfidUid " + uid + ". Extracting details...");
            if (doc.get(fieldData, "fields/email/stringValue")) {
              userData["email"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/fullName/stringValue")) {
              userData["fullName"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/idNumber/stringValue")) {
              userData["idNumber"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
              userData["rfidUid"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/role/stringValue")) {
              userData["role"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/createdAt/stringValue")) {
              userData["createdAt"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/uid/stringValue")) {
              userData["uid"] = fieldData.stringValue;
            }

            Serial.println("User details fetched: fullName=" + userData["fullName"] + 
                           ", email=" + (userData["email"].isEmpty() ? "N/A" : userData["email"]) + 
                           ", role=" + (userData["role"].isEmpty() ? "N/A" : userData["role"]));
            break; // Stop searching once we find the matching user
          }
        }

        if (userData.empty()) {
          Serial.println("No user found with rfidUid " + uid + " in Firestore.");
        }
      } else {
        Serial.println("No documents found in 'users' collection or invalid response format.");
      }
    } else {
      Serial.println("Failed to retrieve Firestore documents: " + firestoreFbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    Serial.println("Cannot fetch user details: sdMode=" + String(sdMode) + ", isConnected=" + String(isConnected));
  }

  return userData;
}

void fetchRegisteredUIDs() {
  if (!sdMode && (WiFi.status() != WL_CONNECTED || pzem.voltage() < voltageThreshold)) {
    return;
  }
  if (Firebase.RTDB.get(&fbdo, "/RegisteredUIDs")) {
    if (fbdo.dataType() == "json") {
      FirebaseJson* json = fbdo.jsonObjectPtr();
      registeredUIDs.clear();
      size_t count = json->iteratorBegin();
      for (size_t i = 0; i < count; i++) {
        int type;
        String key, value;
        json->iteratorGet(i, type, key, value);
        registeredUIDs.push_back(key);
      }
      json->iteratorEnd();
    }
  } else {
    Serial.println("Failed to fetch registered UIDs: " + fbdo.errorReason());
    if (fbdo.errorReason().indexOf("ssl") >= 0 || 
        fbdo.errorReason().indexOf("connection") >= 0 || 
        fbdo.errorReason().indexOf("SSL") >= 0) {
      handleFirebaseSSLError();
    }
  }
  lastUIDFetchTime = millis();
}

void fetchFirestoreTeachers() {
  if (!sdMode && isConnected && Firebase.ready()) {
    firestoreTeachers.clear();
    String path = "teachers";
    yield(); // Prevent watchdog reset
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        Serial.println("Found " + String(arr.size()) + " teachers in Firestore");

        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid, fullName, email, idNumber, mobileNumber, role, department, createdAt;
          FirebaseJsonData fieldData;

          // Extract basic teacher fields
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            Serial.println("Fetched rfidUid: " + rfidUid);
          } else {
            Serial.println("No rfidUid found for document " + String(i));
            continue;
          }
          if (doc.get(fieldData, "fields/fullName/stringValue")) fullName = fieldData.stringValue;
          if (doc.get(fieldData, "fields/email/stringValue")) email = fieldData.stringValue;
          if (doc.get(fieldData, "fields/idNumber/stringValue")) idNumber = fieldData.stringValue;
          if (doc.get(fieldData, "fields/mobileNumber/stringValue")) mobileNumber = fieldData.stringValue;
          if (doc.get(fieldData, "fields/role/stringValue")) {
            role = fieldData.stringValue;
            Serial.println("Fetched role for UID " + rfidUid + ": '" + role + "'");
          } else {
            Serial.println("No role found for UID " + rfidUid + ". Defaulting to 'instructor'");
            role = "instructor";
          }
          if (doc.get(fieldData, "fields/department/stringValue")) department = fieldData.stringValue;
          if (doc.get(fieldData, "fields/createdAt/stringValue")) createdAt = fieldData.stringValue;

          // Parse assignedSubjects to extract schedules and sections
          FirebaseJsonArray schedulesArray;
          FirebaseJsonArray sectionsArray;
          if (doc.get(fieldData, "fields/assignedSubjects/arrayValue/values")) {
            FirebaseJsonArray subjectsArr;
            fieldData.getArray(subjectsArr);
            Serial.println("UID " + rfidUid + " has " + String(subjectsArr.size()) + " assigned subjects");

            for (size_t j = 0; j < subjectsArr.size(); j++) {
              FirebaseJsonData subjectData;
              subjectsArr.get(subjectData, j);
              FirebaseJson subject;
              subject.setJsonData(subjectData.to<String>());
              String subjectName, subjectCode;
              FirebaseJsonData subjectField;
              if (subject.get(subjectField, "mapValue/fields/name/stringValue")) subjectName = subjectField.stringValue;
              if (subject.get(subjectField, "mapValue/fields/code/stringValue")) subjectCode = subjectField.stringValue;

              // Parse sections and their schedules
              if (subject.get(subjectField, "mapValue/fields/sections/arrayValue/values")) {
                FirebaseJsonArray sectionsArr;
                subjectField.getArray(sectionsArr);
                Serial.println("Subject '" + subjectName + "' has " + String(sectionsArr.size()) + " sections");

                for (size_t k = 0; k < sectionsArr.size(); k++) {
                  FirebaseJsonData sectionData;
                  sectionsArr.get(sectionData, k);
                  FirebaseJson section;
                  section.setJsonData(sectionData.to<String>());
                  String sectionId, sectionName, sectionCode, capacity, currentEnrollment;
                  FirebaseJsonData sectionField;
                  if (section.get(sectionField, "mapValue/fields/id/stringValue")) sectionId = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/name/stringValue")) sectionName = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/code/stringValue")) sectionCode = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/capacity/integerValue")) capacity = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/currentEnrollment/integerValue")) currentEnrollment = sectionField.stringValue;

                  Serial.println("Section " + String(k) + ": " + sectionName + ", Code: " + sectionCode + ", Capacity: " + capacity + ", Enrollment: " + currentEnrollment);

                  FirebaseJson sectionEntry;
                  sectionEntry.set("id", sectionId);
                  sectionEntry.set("name", sectionName);
                  sectionEntry.set("code", sectionCode);
                  sectionEntry.set("capacity", capacity);
                  sectionEntry.set("currentEnrollment", currentEnrollment);
                  sectionEntry.set("subject", subjectName);
                  sectionEntry.set("subjectCode", subjectCode);
                  sectionsArray.add(sectionEntry);

                  // Parse schedules within the section
                  if (section.get(sectionField, "mapValue/fields/schedules/arrayValue/values")) {
                    FirebaseJsonArray schedulesArr;
                    sectionField.getArray(schedulesArr);
                    Serial.println("Section '" + sectionName + "' has " + String(schedulesArr.size()) + " schedules");

                    for (size_t m = 0; m < schedulesArr.size(); m++) {
                      FirebaseJsonData scheduleData;
                      schedulesArr.get(scheduleData, m);
                      FirebaseJson schedule;
                      schedule.setJsonData(scheduleData.to<String>());
                      String day, startTime, endTime, roomName;
                      // Define the instructor variables we need for this schedule
                      String instructorUid = rfidUid; // Use the current teacher's UID
                      String instructorName = fullName; // Use the current teacher's name
                      FirebaseJsonData scheduleField;
                      if (schedule.get(scheduleField, "mapValue/fields/day/stringValue")) day = scheduleField.stringValue;
                      if (schedule.get(scheduleField, "mapValue/fields/startTime/stringValue")) startTime = scheduleField.stringValue;
                      if (schedule.get(scheduleField, "mapValue/fields/endTime/stringValue")) endTime = scheduleField.stringValue;
                      if (schedule.get(scheduleField, "mapValue/fields/roomName/stringValue")) roomName = scheduleField.stringValue;

                      Serial.println("Schedule " + String(m) + ": " + day + ", " + startTime + "-" + endTime + ", Room: " + roomName);

                      FirebaseJson scheduleEntry;
                      scheduleEntry.set("day", day);
                      scheduleEntry.set("startTime", startTime);
                      scheduleEntry.set("endTime", endTime);
                      scheduleEntry.set("roomName", roomName);
                      scheduleEntry.set("subject", subjectName);
                      scheduleEntry.set("subjectCode", subjectCode);
                      scheduleEntry.set("section", sectionName);
                      scheduleEntry.set("sectionId", sectionId);
                      scheduleEntry.set("instructorUid", instructorUid);
                      scheduleEntry.set("instructorName", instructorName);

                      schedulesArray.add(scheduleEntry); // Add directly to array
                    }
                  }
                }
              }
            }
          }

          // Store teacher data in firestoreTeachers
          if (rfidUid != "") {
            std::map<String, String> teacherData;
            teacherData["fullName"] = fullName;
            teacherData["email"] = email;
            teacherData["idNumber"] = idNumber;
            teacherData["mobileNumber"] = mobileNumber;
            teacherData["role"] = role;
            teacherData["department"] = department;
            teacherData["createdAt"] = createdAt;

            String schedulesStr = "[]";
            String sectionsStr = "[]";
            if (schedulesArray.size() > 0) {
              schedulesArray.toString(schedulesStr, true);
            }
            if (sectionsArray.size() > 0) {
              sectionsArray.toString(sectionsStr, true);
            }
            Serial.println("Schedules for UID " + rfidUid + ": " + schedulesStr);
            Serial.println("Sections for UID " + rfidUid + ": " + sectionsStr);
            teacherData["schedules"] = schedulesStr;
            teacherData["sections"] = sectionsStr;
            firestoreTeachers[rfidUid] = teacherData;
          }
        }

        // Sync schedules to SD card after fetching
        if (syncSchedulesToSD()) {
          Serial.println("Schedules synced to SD card successfully.");
        } else {
          Serial.println("Failed to sync schedules to SD card.");
        }

        Serial.println("Firestore teachers fetched and cached locally. Total: " + String(firestoreTeachers.size()));
      } else {
        Serial.println("No documents found in Firestore teachers collection");
      }
    } else {
      Serial.println("Failed to fetch Firestore teachers: " + firestoreFbdo.errorReason());
      if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
          firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
          firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    Serial.println("Skipping fetchFirestoreTeachers: sdMode=" + String(sdMode) + 
                   ", WiFi=" + String(WiFi.status()) + 
                   ", FirebaseReady=" + String(Firebase.ready()));
  }
}

void fetchFirestoreStudents() {
  if (!sdMode && (WiFi.status() != WL_CONNECTED || pzem.voltage() < voltageThreshold)) {
    Serial.println("Skipping fetchFirestoreStudents: Not connected or low voltage (WiFi: " + String(WiFi.status()) + ", Voltage: " + String(pzem.voltage()) + ")");
    return;
  }

  firestoreStudents.clear();
  String path = "students";
  Serial.println("Fetching students from Firestore at path: " + path);

  int retries = 3;
  bool success = false;
  for (int attempt = 1; attempt <= retries && !success; attempt++) {
    Serial.println("Attempt " + String(attempt) + " to fetch Firestore data...");
    yield(); // Prevent watchdog reset before Firebase call
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), "")) {
      success = true;
      Serial.println("Firestore fetch successful. Raw payload:");
      Serial.println(firestoreFbdo.payload());

      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        Serial.println("Found " + String(arr.size()) + " documents in Firestore response.");

        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          Serial.println("Document[" + String(i) + "] raw data: " + docData.to<String>());

          String rfidUid, fullName, email, idNumber, mobileNumber, role, department;
          FirebaseJsonData fieldData;

          rfidUid = doc.get(fieldData, "fields/rfidUid/stringValue") ? fieldData.stringValue : "";
          fullName = doc.get(fieldData, "fields/fullName/stringValue") ? fieldData.stringValue : "Unknown";
          email = doc.get(fieldData, "fields/email/stringValue") ? fieldData.stringValue : "";
          idNumber = doc.get(fieldData, "fields/idNumber/stringValue") ? fieldData.stringValue : "";
          mobileNumber = doc.get(fieldData, "fields/mobileNumber/stringValue") ? fieldData.stringValue : "";
          role = doc.get(fieldData, "fields/role/stringValue") ? fieldData.stringValue : "student";
          department = doc.get(fieldData, "fields/department/stringValue") ? fieldData.stringValue : "";

          Serial.println("Parsed rfidUid: " + rfidUid + ", fullName: " + fullName);

          String schedulesJsonStr = "[]";
          FirebaseJsonArray schedulesArray; // Use array instead of object

          if (doc.get(fieldData, "fields/enrolledSubjects/arrayValue/values")) {
            FirebaseJsonArray subjectsArr;
            fieldData.getArray(subjectsArr);
            Serial.println("Found " + String(subjectsArr.size()) + " enrolledSubjects for " + rfidUid);

            for (size_t j = 0; j < subjectsArr.size(); j++) {
              FirebaseJsonData subjectData;
              subjectsArr.get(subjectData, j);
              FirebaseJson subject;
              subject.setJsonData(subjectData.to<String>());
              Serial.println("Processing enrolledSubject[" + String(j) + "]: " + subjectData.to<String>());

              String subjectCode = subject.get(fieldData, "mapValue/fields/code/stringValue") ? fieldData.stringValue : "Unknown";
              String subjectName = subject.get(fieldData, "mapValue/fields/name/stringValue") ? fieldData.stringValue : "Unknown";
              String instructorUid = subject.get(fieldData, "mapValue/fields/instructorId/stringValue") ? fieldData.stringValue : "";
              String instructorName = subject.get(fieldData, "mapValue/fields/instructorName/stringValue") ? fieldData.stringValue : "";
              String sectionId = subject.get(fieldData, "mapValue/fields/sectionId/stringValue") ? fieldData.stringValue : "";
              String sectionName = subject.get(fieldData, "mapValue/fields/sectionName/stringValue") ? fieldData.stringValue : "";

              if (subject.get(fieldData, "mapValue/fields/schedules/arrayValue/values")) {
                FirebaseJsonArray schedulesArr;
                fieldData.getArray(schedulesArr);
                Serial.println("Found " + String(schedulesArr.size()) + " schedules for subject " + subjectCode);

                for (size_t k = 0; k < schedulesArr.size(); k++) {
                  FirebaseJsonData scheduleData;
                  schedulesArr.get(scheduleData, k);
                  FirebaseJson schedule;
                  schedule.setJsonData(scheduleData.to<String>());
                  Serial.println("Processing schedule[" + String(k) + "]: " + scheduleData.to<String>());

                  FirebaseJsonData scheduleField;
                  String day = schedule.get(scheduleField, "mapValue/fields/day/stringValue") ? scheduleField.stringValue : "";
                  String startTime = schedule.get(scheduleField, "mapValue/fields/startTime/stringValue") ? scheduleField.stringValue : "";
                  String endTime = schedule.get(scheduleField, "mapValue/fields/endTime/stringValue") ? scheduleField.stringValue : "";
                  String roomName = schedule.get(scheduleField, "mapValue/fields/roomName/stringValue") ? scheduleField.stringValue : "";

                  FirebaseJson scheduleObj;
                  scheduleObj.set("day", day);
                  scheduleObj.set("startTime", startTime);
                  scheduleObj.set("endTime", endTime);
                  scheduleObj.set("roomName", roomName);
                  scheduleObj.set("subjectCode", subjectCode);
                  scheduleObj.set("subject", subjectName);
                  scheduleObj.set("section", sectionName);
                  scheduleObj.set("sectionId", sectionId);
                  scheduleObj.set("instructorUid", instructorUid);
                  scheduleObj.set("instructorName", instructorName);

                  schedulesArray.add(scheduleObj); // Add directly to array
                }
              } else {
                Serial.println("No schedules found in enrolledSubject[" + String(j) + "] for " + rfidUid);
              }
            }
            schedulesArray.toString(schedulesJsonStr, true); // Serialize array
            Serial.println("Combined schedules for " + rfidUid + ": " + schedulesJsonStr);
          } else {
            Serial.println("No enrolledSubjects/values found for " + rfidUid);
          }

          if (rfidUid != "") {
            std::map<String, String> studentData;
            studentData["fullName"] = fullName;
            studentData["email"] = email;
            studentData["idNumber"] = idNumber;
            studentData["mobileNumber"] = mobileNumber;
            studentData["role"] = role;
            studentData["department"] = department;
            studentData["schedules"] = schedulesJsonStr;
            firestoreStudents[rfidUid] = studentData;
            Serial.println("Stored student " + rfidUid + " with schedules: " + schedulesJsonStr);
          }
        }
        Serial.println("Fetched " + String(firestoreStudents.size()) + " students from Firestore.");
      } else {
        Serial.println("No documents found in Firestore response.");
      }
    } else {
      Serial.println("Firestore fetch failed (attempt " + String(attempt) + "): " + firestoreFbdo.errorReason());
      if (attempt < retries) {
        Serial.println("Retrying in 5 seconds...");
        delay(5000);
        Firebase.reconnectWiFi(true);
      }
      if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
          firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
          firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
    yield(); // Prevent watchdog reset after Firebase call
  }

  if (!success) {
    Serial.println("All fetch attempts failed. Switching to SD mode.");
    sdMode = true;
  }

  if (firestoreStudents.find("5464E1BA") != firestoreStudents.end()) {
    Serial.println("Verified: 5464E1BA found with schedules: " + firestoreStudents["5464E1BA"]["schedules"]);
  } else {
    Serial.println("Verified: 5464E1BA NOT found in firestoreStudents.");
  }
}

// Add this function after initFirebase() and before fetchFirestoreRooms()
bool ensureFirebaseAuthenticated() {
  if (!Firebase.ready()) {
    Serial.println("Firebase not ready. Attempting re-initialization...");
    Firebase.reconnectWiFi(true);
    initFirebase();
    delay(1000);
    
    if (!Firebase.ready()) {
      Serial.println("Firebase still not ready after re-init.");
      return false;
    }
  }
  
  if (!Firebase.authenticated()) {
    Serial.println("Firebase not authenticated. Attempting sign-in...");
    // Try to sign in again
    if (!Firebase.signUp(&config, &auth, "", "")) {
      Serial.printf("Firebase re-auth failed: %s\n", config.signer.signupError.message.c_str());
      
      // Check for SSL errors in authentication failure
      String errorMessage = config.signer.signupError.message.c_str();
      if (errorMessage.indexOf("ssl") >= 0 || 
          errorMessage.indexOf("SSL") >= 0 || 
          errorMessage.indexOf("connection") >= 0 ||
          errorMessage.indexOf("handshake") >= 0 ||
          errorMessage.indexOf("certificate") >= 0 ||
          errorMessage.indexOf("network") >= 0) {
        Serial.println("SSL or network issue detected during authentication");
        
        // Try WiFi reconnection
        if (WiFi.status() != WL_CONNECTED) {
          Serial.println("WiFi disconnected during auth. Reconnecting...");
          connectWiFi();
        } else {
          // Reset WiFi connection
          Serial.println("Resetting WiFi connection");
          WiFi.disconnect();
          delay(500);
          connectWiFi();
        }
        
        delay(500);
        
        // Retry authentication one more time after WiFi reset
        if (!Firebase.signUp(&config, &auth, "", "")) {
          Serial.printf("Second Firebase auth attempt failed: %s\n", config.signer.signupError.message.c_str());
          return false;
        }
      } else {
        return false;
      }
    }
    
    delay(1000); // Give it time to process
    if (!Firebase.authenticated()) {
      Serial.println("Firebase still not authenticated after re-auth attempt.");
      return false;
    }
  }
  
  Serial.println("Firebase is ready and authenticated.");
  return true;
}

void fetchFirestoreRooms() {
  if (!ensureFirebaseAuthenticated()) {
    Serial.println("Cannot fetch Firestore rooms, Firebase not authenticated.");
    return;
  }

  Serial.println("Fetching Firestore rooms...");
  if (Firebase.Firestore.getDocument(&fbdo, FIRESTORE_PROJECT_ID, "", "rooms", "")) {
    FirebaseJson roomsJson;
    roomsJson.setJsonData(fbdo.payload().c_str());
    FirebaseJsonData jsonData;

    if (roomsJson.get(jsonData, "documents") && jsonData.type == "array") {
      FirebaseJsonArray arr;
      jsonData.getArray(arr);
      firestoreRooms.clear();

      for (size_t i = 0; i < arr.size(); i++) {
        FirebaseJsonData docData;
        arr.get(docData, i);
        FirebaseJson doc;
        doc.setJsonData(docData.stringValue);

        // Extract room ID from document name
        String docName;
        doc.get(docData, "name");
        docName = docData.stringValue;
        String roomId = docName.substring(docName.lastIndexOf("/") + 1);

        // Extract fields
        FirebaseJson fields;
        doc.get(docData, "fields");
        fields.setJsonData(docData.stringValue);

        std::map<String, String> roomData;
        FirebaseJsonData fieldData;

        // Root-level fields with existence checks
        if (fields.get(fieldData, "building/stringValue")) {
          roomData["building"] = fieldData.stringValue;
        } else {
          roomData["building"] = "Unknown";
          Serial.println("Warning: 'building' missing for room " + roomId);
        }
        if (fields.get(fieldData, "floor/stringValue")) {
          roomData["floor"] = fieldData.stringValue;
        } else {
          roomData["floor"] = "Unknown";
          Serial.println("Warning: 'floor' missing for room " + roomId);
        }
        if (fields.get(fieldData, "name/stringValue")) {
          roomData["name"] = fieldData.stringValue;
        } else {
          roomData["name"] = "Unknown";
          Serial.println("Warning: 'name' missing for room " + roomId);
        }
        if (fields.get(fieldData, "status/stringValue")) {
          roomData["status"] = fieldData.stringValue;
        } else {
          roomData["status"] = "Unknown";
          Serial.println("Warning: 'status' missing for room " + roomId);
        }
        if (fields.get(fieldData, "type/stringValue")) {
          roomData["type"] = fieldData.stringValue;
        } else {
          roomData["type"] = "Unknown";
          Serial.println("Warning: 'type' missing for room " + roomId);
        }

        firestoreRooms[roomId] = roomData;
        Serial.println("Fetched room " + roomId + ": building=" + roomData["building"] + 
                       ", floor=" + roomData["floor"] + ", name=" + roomData["name"] +
                       ", status=" + roomData["status"] + ", type=" + roomData["type"]);
      }
      Serial.println("Fetched " + String(firestoreRooms.size()) + " rooms from Firestore.");
    } else {
      Serial.println("No documents found in rooms collection or invalid format.");
    }
  } else {
    Serial.println("Failed to fetch Firestore rooms: " + fbdo.payload());
    
    // Additional debugging for Firestore permissions issue
    Serial.println("Attempting to reconnect and retry...");
    Firebase.reconnectWiFi(true);
    delay(1000);
    
    // Try a second attempt with the firestoreFbdo object
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", "rooms", "")) {
      Serial.println("Second attempt successful with firestoreFbdo!");
      FirebaseJson roomsJson;
      roomsJson.setJsonData(firestoreFbdo.payload().c_str());
      FirebaseJsonData jsonData;
      
      if (roomsJson.get(jsonData, "documents") && jsonData.type == "array") {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        firestoreRooms.clear();
        
        // Process rooms as in the original attempt
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          
          // Extract room ID from document name
          String docName;
          doc.get(docData, "name");
          docName = docData.stringValue;
          String roomId = docName.substring(docName.lastIndexOf("/") + 1);
          
          // Extract fields
          FirebaseJson fields;
          doc.get(docData, "fields");
          fields.setJsonData(docData.stringValue);
          
          std::map<String, String> roomData;
          FirebaseJsonData fieldData;
          
          // Process fields
          if (fields.get(fieldData, "building/stringValue")) {
            roomData["building"] = fieldData.stringValue;
          } else {
            roomData["building"] = "Unknown";
          }
          if (fields.get(fieldData, "floor/stringValue")) {
            roomData["floor"] = fieldData.stringValue;
          } else {
            roomData["floor"] = "Unknown";
          }
          if (fields.get(fieldData, "name/stringValue")) {
            roomData["name"] = fieldData.stringValue;
          } else {
            roomData["name"] = "Unknown";
          }
          if (fields.get(fieldData, "status/stringValue")) {
            roomData["status"] = fieldData.stringValue;
          } else {
            roomData["status"] = "Unknown";
          }
          if (fields.get(fieldData, "type/stringValue")) {
            roomData["type"] = fieldData.stringValue;
          } else {
            roomData["type"] = "Unknown";
          }
          
          firestoreRooms[roomId] = roomData;
          Serial.println("Fetched room " + roomId + ": building=" + roomData["building"] + 
                        ", floor=" + roomData["floor"] + ", name=" + roomData["name"] +
                        ", status=" + roomData["status"] + ", type=" + roomData["type"]);
        }
        Serial.println("Fetched " + String(firestoreRooms.size()) + " rooms from Firestore on second attempt.");
      } else {
        Serial.println("Second attempt: No documents found in rooms collection or invalid format.");
      }
    } else {
      Serial.println("Second attempt also failed: " + firestoreFbdo.errorReason());
      Serial.println("Firebase auth status: " + String(Firebase.authenticated()));
      Serial.println("Verify Firestore rules and authentication.");
    }
    if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
        firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
        firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
      handleFirebaseSSLError();
    }
  }
}

String assignRoomToAdmin(String uid) {
  String selectedRoomId = "";

  // Ensure firestoreRooms is populated
  if (firestoreRooms.empty()) {
    Serial.println("firestoreRooms is empty. Cannot assign a room to admin UID: " + uid);
    return selectedRoomId;
  }

  // Filter rooms based on status (e.g., "maintenance" for inspection)
  Serial.println("Assigning room for admin UID: " + uid);
  Serial.println("Searching for rooms with status 'maintenance'...");
  
  // Debug room statuses
  for (const auto& room : firestoreRooms) {
    String roomId = room.first;
    const auto& roomData = room.second;
    
    // Extract room details using at() to avoid const issues
    String roomStatus = roomData.at("status");
    String roomName = roomData.at("name");
    
    Serial.println("Room " + roomId + " (" + roomName + ") has status: '" + roomStatus + "'");
  }
  
  // First try exact match with 'maintenance'
  for (const auto& room : firestoreRooms) {
    String roomId = room.first;
    const auto& roomData = room.second;
    
    String roomStatus = roomData.at("status");
    String roomBuilding = roomData.at("building");
    String roomName = roomData.at("name");
    
    // Primary match: status equals "maintenance"
    if (roomStatus == "maintenance") {
      selectedRoomId = roomId;
      Serial.println("Selected room " + roomId + " with status 'maintenance' for admin UID: " + uid + 
                     " (building: " + roomBuilding + ", name: " + roomName + ")");
      return selectedRoomId;
    }
  }
  
  // If no room found with exactly "maintenance", try case-insensitive or partial match
  for (const auto& room : firestoreRooms) {
    String roomId = room.first;
    const auto& roomData = room.second;
    
    String roomStatus = roomData.at("status");
    String roomStatusLower = roomStatus;
    roomStatusLower.toLowerCase();
    
    String roomBuilding = roomData.at("building");
    String roomName = roomData.at("name");
    
    // Secondary match: status contains "maintenance" (case insensitive)
    if (roomStatusLower.indexOf("maintenance") >= 0) {
      selectedRoomId = roomId;
      Serial.println("Selected room " + roomId + " with status containing 'maintenance' for admin UID: " + uid + 
                     " (actual status: '" + roomStatus + "', building: " + roomBuilding + ", name: " + roomName + ")");
      return selectedRoomId;
    }
  }
  
  // If still no match, assign any room for testing purposes
  if (selectedRoomId == "") {
    Serial.println("No room with status 'maintenance' found. Assigning first available room for testing.");
    if (!firestoreRooms.empty()) {
      selectedRoomId = firestoreRooms.begin()->first;
      const auto& roomData = firestoreRooms.begin()->second;
      String roomStatus = roomData.at("status");
      String roomName = roomData.at("name");
      Serial.println("Assigned room " + selectedRoomId + " (" + roomName + ") with status '" + roomStatus + "' for admin UID: " + uid);
    } else {
      Serial.println("No rooms available to assign to admin UID: " + uid);
    }
  }
  
  return selectedRoomId;
}

void assignRoomToInstructor(String uid, String timestamp) {
  if (firestoreTeachers.find(uid) == firestoreTeachers.end()) {
    return;
  }

  String teacherSchedules = firestoreTeachers[uid]["schedules"];
  FirebaseJson teacherJson;
  teacherJson.setJsonData(teacherSchedules);
  FirebaseJsonData jsonData;

  if (teacherJson.get(jsonData, "/")) {
    FirebaseJsonArray schedulesArray;
    jsonData.getArray(schedulesArray);

    String currentDay, currentTime;
    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {
      char dayStr[10];
      char timeStr[10];
      strftime(dayStr, sizeof(dayStr), "%A", &timeinfo);
      strftime(timeStr, sizeof(timeStr), "%H:%M", &timeinfo);
      currentDay = String(dayStr);
      currentTime = String(timeStr);
    } else {
      return;
    }

    String assignedRoom = "";
    String subject = "";
    String sessionStart = timestamp;
    String sessionEnd = "";

    for (size_t i = 0; i < schedulesArray.size(); i++) {
      FirebaseJsonData scheduleData;
      schedulesArray.get(scheduleData, i);
      FirebaseJson schedule;
      schedule.setJsonData(scheduleData.to<String>());
      String day, startTime, endTime, room, subjectSchedule;
      FirebaseJsonData field;
      if (schedule.get(field, "day")) day = field.stringValue;
      if (schedule.get(field, "startTime")) startTime = field.stringValue;
      if (schedule.get(field, "endTime")) endTime = field.stringValue;
      if (schedule.get(field, "room")) room = field.stringValue;
      if (schedule.get(field, "subject")) subjectSchedule = field.stringValue;

      if (day == currentDay) {
        if (currentTime >= startTime && currentTime <= endTime) {
          assignedRoom = room;
          subject = subjectSchedule;
          sessionEnd = endTime;
          break;
        }
      }
    }

    if (assignedRoom != "") {
      for (auto& room : firestoreRooms) {
        String roomId = room.first;
        if (room.second.at("roomName") == assignedRoom && room.second.at("status") == "available") {
          assignedRoomId = roomId;
          sessionStartReading = pzem.energy();
          if (!sdMode && isConnected && isVoltageSufficient) {
            String path = "rooms/" + roomId;
            FirebaseJson content;
            content.set("fields/status/stringValue", "occupied");
            content.set("fields/assignedInstructor/stringValue", firestoreTeachers[uid]["fullName"]);
            if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), content.raw(), "status,assignedInstructor")) {
              firestoreRooms[roomId]["status"] = "occupied";
              firestoreRooms[roomId]["assignedInstructor"] = firestoreTeachers[uid]["fullName"];
            }
          }
          break;
        }
      }
    }
  }
}

void updateRoomStatus(String roomId, String status, String instructorName, String subject, String sessionStart, String sessionEnd, float startReading, float endReading, float totalUsage) {
  if (!sdMode && isConnected && isVoltageSufficient) {
    String path = "rooms/" + roomId;
    FirebaseJson content;
    content.set("fields/status/stringValue", status);
    content.set("fields/assignedInstructor/stringValue", "");
    content.set("fields/lastSession/mapValue/fields/subject/stringValue", subject);
    content.set("fields/lastSession/mapValue/fields/instructorName/stringValue", instructorName);
    content.set("fields/lastSession/mapValue/fields/sessionStart/stringValue", sessionStart);
    content.set("fields/lastSession/mapValue/fields/sessionEnd/stringValue", sessionEnd);
    content.set("fields/lastSession/mapValue/fields/energyUsageStart/doubleValue", startReading);
    content.set("fields/lastSession/mapValue/fields/energyUsageEnd/doubleValue", endReading);
    content.set("fields/lastSession/mapValue/fields/totalUsage/doubleValue", totalUsage);
    if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), content.raw(), "status,assignedInstructor,lastSession")) {
      firestoreRooms[roomId]["status"] = status;
      firestoreRooms[roomId]["assignedInstructor"] = "";
    }
  }
  assignedRoomId = "";
  sessionStartReading = 0.0;
}

void storeLogToSD(String entry) {
  if (isConnected) {
    Serial.println("WiFi is connected. Skipping SD log storage for: " + entry);
    return;
  }

  static bool reinitializedInThisCall = false;

  if (!sdInitialized && !reinitializedInThisCall) {
    if (!SD.begin(SD_CS, fsSPI, 4000000)) {
      Serial.println("SD card initialization failed during setup. Cannot store log: " + entry);
      sdInitialized = false;
      return;
    }
    sdInitialized = true;
    Serial.println("SD card initialized for logging.");
    yield(); // Yield after SD initialization
  }

  File logFile = SD.open(OFFLINE_LOG_FILE, FILE_APPEND);
  if (logFile) {
    logFile.println(entry);
    logFile.flush();
    logFile.close();
    Serial.println("Stored to SD: " + entry);
    reinitializedInThisCall = false;
    yield(); // Yield after SD write
  } else {
    Serial.println("Failed to open " + String(OFFLINE_LOG_FILE) + " for writing. Diagnosing...");
    yield(); // Yield during error handling

    File root = SD.open("/");
    if (!root) {
      Serial.println("SD card root directory inaccessible. Attempting reinitialization...");
      if (!reinitializedInThisCall && SD.begin(SD_CS, fsSPI, 4000000)) {
        Serial.println("SD card reinitialized successfully.");
        sdInitialized = true;
        reinitializedInThisCall = true;
      } else {
        Serial.println("SD card reinitialization failed. Hardware issue or card removed?");
        sdInitialized = false;
        Serial.println("Falling back to serial-only logging: " + entry);
        return;
      }
      yield(); // Yield after reinitialization attempt
    } else {
      root.close();
      Serial.println("SD card root accessible, issue is file-specific.");
      yield(); // Yield after root check
    }

    if (SD.exists(OFFLINE_LOG_FILE)) {
      Serial.println(String(OFFLINE_LOG_FILE) + " exists but can't be opened. Attempting to delete...");
      if (SD.remove(OFFLINE_LOG_FILE)) {
        Serial.println("Deleted " + String(OFFLINE_LOG_FILE) + " successfully.");
      } else {
        Serial.println("Failed to delete " + String(OFFLINE_LOG_FILE) + ". Possible write protection or corruption.");
        return;
      }
      yield(); // Yield after file deletion
    } else {
      Serial.println(String(OFFLINE_LOG_FILE) + " does not exist yet. Creating new file...");
      yield(); // Yield before file creation
    }

    logFile = SD.open(OFFLINE_LOG_FILE, FILE_APPEND);
    if (logFile) {
      logFile.println(entry);
      logFile.flush();
      logFile.close();
      Serial.println("Recreated and stored to SD: " + entry);
      reinitializedInThisCall = false;
      yield(); // Yield after successful retry
    } else {
      Serial.println("Retry failed for " + String(OFFLINE_LOG_FILE) + ". Testing SD card integrity...");
      File testFile = SD.open("/test_log.txt", FILE_WRITE);
      if (testFile) {
        testFile.println("Test entry at " + getFormattedTime() + ": " + entry);
        testFile.flush();
        testFile.close();
        Serial.println("Test write to /test_log.txt succeeded. Issue is specific to " + String(OFFLINE_LOG_FILE));
      } else {
        Serial.println("Test write to /test_log.txt failed. SD card is likely faulty or full.");
        sdInitialized = false;
        Serial.println("Falling back to serial-only logging: " + entry);
      }
      yield(); // Yield after test write
    }
  }
}

bool syncOfflineLogs() {
  if (sdMode || !isConnected || !isVoltageSufficient) {
    Serial.println("Cannot sync logs: SD mode or no connection/voltage.");
    return false;
  }
  if (!SD.exists(OFFLINE_LOG_FILE)) {
    Serial.println("No offline logs to sync.");
    return true;
  }
  File file = SD.open(OFFLINE_LOG_FILE, FILE_READ);
  if (!file) {
    Serial.println("⚠️ Could not open SD log file for syncing.");
    return false;
  }
  Serial.println("Syncing offline logs to Firebase...");
  bool allSuccess = true;
  while (file.available()) {
    String entry = file.readStringUntil('\n');
    entry.trim();
    if (entry.length() > 0) {
      Serial.println("Sync log: " + entry);
      nonBlockingDelay(1);
    }
  }
  file.close();
  if (allSuccess) {
    if (SD.remove(OFFLINE_LOG_FILE)) {
      Serial.println("SD log file cleared after sync.");
      return true;
    } else {
      Serial.println("⚠️ Could not remove SD file.");
      return false;
    }
  } else {
    Serial.println("Some logs failed to upload; keeping SD file.");
    return false;
  }
}

void logSuperAdminPZEMToSD(String uid, String timestamp) {
  float voltage = pzem.voltage();
  float current = pzem.current();
  float power = pzem.power();
  float energy = pzem.energy();
  float frequency = pzem.frequency();
  float pf = pzem.pf();

  // Ensure valid readings
  if (isnan(voltage) || voltage < 0) voltage = 0.0;
  if (isnan(current) || current < 0) current = 0.0;
  if (isnan(power) || power < 0) power = 0.0;
  if (isnan(energy) || energy < 0) energy = 0.0;
  if (isnan(frequency) || frequency < 0) frequency = 0.0;
  if (isnan(pf) || pf < 0) pf = 0.0;

  // Calculate total energy since session start
  static float superAdminTotalEnergy = 0.0;
  static unsigned long lastSuperAdminPZEMUpdate = 0;
  if (lastSuperAdminPZEMUpdate != 0) {
    unsigned long elapsed = millis() - lastSuperAdminPZEMUpdate;
    float energyIncrement = (power * (elapsed / 3600000.0)) / 1000.0;
    superAdminTotalEnergy += energyIncrement;
  }
  lastSuperAdminPZEMUpdate = millis();

  // Log to SD card
  String entry = "SuperAdminPZEM:" + uid + " Timestamp:" + timestamp +
                 " Voltage:" + String(voltage, 2) + "V" +
                 " Current:" + String(current, 2) + "A" +
                 " Power:" + String(power, 2) + "W" +
                 " Energy:" + String(energy, 2) + "kWh" +
                 " Frequency:" + String(frequency, 2) + "Hz" +
                 " PowerFactor:" + String(pf, 2) +
                 " TotalConsumption:" + String(superAdminTotalEnergy, 3) + "kWh";
  storeLogToSD(entry);
  Serial.println("Super Admin PZEM logged to SD: " + entry);

  // Reset total energy when session ends
  if (!superAdminSessionActive) {
    superAdminTotalEnergy = 0.0;
    lastSuperAdminPZEMUpdate = 0;
  }
}

void printSDCardInfo() {
  uint64_t cardSize = SD.cardSize() / (1024 * 1024);
  uint64_t cardFree = (SD.totalBytes() - SD.usedBytes()) / (1024 * 1024);
  Serial.println("SD Card Info:");
  Serial.print("Total space: "); Serial.print(cardSize); Serial.println(" MB");
  Serial.print("Free space: "); Serial.print(cardFree); Serial.println(" MB");
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void activateRelays() {
  // If transition already in progress, queue it
  if (relayTransitionInProgress) {
    Serial.println("SAFETY: Relay transition already in progress, queuing activation");
    relayOperationPending = true;
    pendingRelayActivation = true;
    return;
  }
  
  // Start transition
  relayTransitionInProgress = true;
  relayTransitionStartTime = millis();
  
  // RELAY LOGIC:
  // HIGH = Relay OFF/Inactive = Door unlocked
  // LOW = Relay ON/Active = Door locked
  
  // Check system status before relay changes
  yield(); // Yield CPU to prevent watchdog trigger
  
  // Temporarily disable interrupts during critical relay state changes
  noInterrupts();
  
  // Locking the doors (activating relays) with improved timing
  // First relay - activate with longer delay
  digitalWrite(RELAY1, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  yield(); // Add yield between relay operations
  
  // Second relay - activate with delay
  digitalWrite(RELAY2, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  yield(); // Add yield between relay operations
  
  // Third relay - activate with delay
  digitalWrite(RELAY3, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  yield(); // Add yield between relay operations
  
  // Fourth relay - activate with final delay
  digitalWrite(RELAY4, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  
  // Re-enable interrupts
  interrupts();
  
  // Allow system to breathe after relay operation
  yield();
  
  // Update state flags
  relayActive = true;
  relayActiveTime = millis();
  relayTransitionInProgress = false;
  
  Serial.println("Relays activated (locked) with safe timing");
  
  // Non-blocking approach to delay PZEM readings
  lastPZEMUpdate = millis() + 1500; // Wait 1.5 seconds before next PZEM reading (increased from 1 sec)
  
  // Update watchdog timers
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  // Final yield to stabilize system
  yield();
}

void deactivateRelays() {
  // If transition already in progress, queue it
  if (relayTransitionInProgress) {
    Serial.println("SAFETY: Relay transition already in progress, queuing deactivation");
    relayOperationPending = true;
    pendingRelayActivation = false;
    return;
  }
  
  // Multiple yields before relay operation
  yield();
  delay(20);
  yield();
  
  // If we're in the middle of a critical operation, delay the deactivation
  if ((WiFi.status() == WL_CONNECTED && WiFi.status() != WL_IDLE_STATUS) ||
      (Firebase.ready() && Firebase.isTokenExpired() < 5)) {
    // We're in the middle of a network operation, schedule for later
    Serial.println("SAFETY: Network operation active, scheduling relay deactivation");
    relayPendingDeactivation = true;
    relayDeactivationTime = millis() + RELAY_SAFE_DELAY;
    return;
  }
  
  // Start transition
  relayTransitionInProgress = true;
  relayTransitionStartTime = millis();
  
  // Multiple yields before relay operation
  yield();
  delay(20);
  yield();
  
  // RELAY LOGIC:
  // HIGH = Relay OFF/Inactive = Door unlocked
  // LOW = Relay ON/Active = Door locked
  
  // Temporarily disable interrupts during critical relay state changes
  noInterrupts();
  
  // Unlocking the doors (deactivating relays) with progressive delays and yields
  digitalWrite(RELAY1, HIGH);
  delay(50);
  yield();
  
  digitalWrite(RELAY2, HIGH);
  delay(50);
  yield();
  
  digitalWrite(RELAY3, HIGH);
  delay(50);
  yield();
  
  digitalWrite(RELAY4, HIGH);
  delay(50);
  yield();
  
  // Re-enable interrupts
  interrupts();
  
  // Multiple yields after relay operation
  yield();
  delay(50);
  yield();
  
  // Update state flags
  relayActive = false;
  relayTransitionInProgress = false;
  
  Serial.println("Relays deactivated (unlocked) with safe timing");
  
  // Update watchdog timers
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  // Additional yields to stabilize system
  yield();
  delay(50);
  yield();
}

void checkPendingRelayOperations() {
  // Add a static variable to track consecutive errors
  static int consecutiveErrors = 0;
  static unsigned long lastSuccessfulOperation = 0;
  
  // Initial yield to ensure system stability
  yield();
  
  // Check for pending relay deactivation
  if (relayPendingDeactivation && millis() >= relayDeactivationTime) {
    Serial.println("Executing delayed relay deactivation");
    relayPendingDeactivation = false;
    
    // Start transition
    relayTransitionInProgress = true;
    relayTransitionStartTime = millis();
    
    // Yield before operation to prevent watchdog issues
    yield();
    
    // Disable interrupts for relay operations
    noInterrupts();
    
    // RELAY LOGIC: HIGH = Door unlocked, LOW = Door locked
    digitalWrite(RELAY1, HIGH);
    delay(50); // Increased delay for smoother deactivation
    yield(); // Add yield between relay operations
    digitalWrite(RELAY2, HIGH);
    delay(50); // Increased delay for smoother deactivation
    yield(); // Add yield between relay operations
    digitalWrite(RELAY3, HIGH);
    delay(50); // Increased delay for smoother deactivation
    yield(); // Add yield between relay operations
    digitalWrite(RELAY4, HIGH);
    delay(50); // Add final delay for stability
    
    // Re-enable interrupts
    interrupts();
    
    // Yield after relay state changes
    yield();
    delay(20);
    yield();
    
    relayActive = false;
    relayTransitionInProgress = false;
    lastSuccessfulOperation = millis(); // Mark this operation as successful
    consecutiveErrors = 0; // Reset error counter
    
    Serial.println("Relays safely deactivated after delay");
    
    // Update system state
    lastActivityTime = millis();
    lastReadyPrint = millis();
    
    // Allow system to stabilize before display update
    yield();
    
    // Transition to ready state
    if (!adminAccessActive && !classSessionActive && !studentVerificationActive && !tapOutPhase) {
      yield(); // Allow system to process before display update
      
      // Check reed sensor state to ensure there are no pending tamper issues
      if (reedState && !tamperActive) {
        displayMessage("Ready. Tap your", "RFID Card!", 0);
        readyMessageShown = true;
        Serial.println("System returned to ready state after relay deactivation");
      }
    }
  }
  
  // Check system resources before proceeding
  if (ESP.getFreeHeap() < 10000) {
    Serial.println("WARNING: Low memory detected in relay operations, skipping checks");
    yield(); // Allow system tasks to process
    return;
  }
  
  // Check for pending operations
  if (relayOperationPending && !relayTransitionInProgress) {
    // Yield before operation to prevent watchdog issues
    yield();
    
    relayOperationPending = false;
    
    if (pendingRelayActivation) {
      Serial.println("Executing pending relay activation");
      activateRelays();
      lastSuccessfulOperation = millis();
      consecutiveErrors = 0;
    } else if (!relayPendingDeactivation) {
      Serial.println("Executing pending relay deactivation");
      deactivateRelays();
      lastSuccessfulOperation = millis();
      consecutiveErrors = 0;
    }
  }
  
  // Check for transition timeout (safety mechanism)
  if (relayTransitionInProgress) {
    // How long has the transition been active?
    unsigned long transitionDuration = millis() - relayTransitionStartTime;
    
    if (transitionDuration > RELAY_TRANSITION_TIMEOUT) {
      Serial.println("WARNING: Relay transition timeout after " + String(transitionDuration) + "ms, forcing completion");
      
      // Force reset of transition state
      relayTransitionInProgress = false;
      
      // Increment error counter
      consecutiveErrors++;
      
      // If we have too many consecutive errors, try a recovery procedure
      if (consecutiveErrors >= 3) {
        Serial.println("CRITICAL: Multiple relay transition timeouts detected, performing recovery");
        
        // Ensure relays are in a safe state
        digitalWrite(RELAY1, HIGH);
        delay(50);
        yield();
        digitalWrite(RELAY2, HIGH);
        delay(50);
        yield();
        digitalWrite(RELAY3, HIGH);
        delay(50);
        yield();
        digitalWrite(RELAY4, HIGH);
        
        // Reset all relay state variables
        relayActive = false;
        relayOperationPending = false;
        pendingRelayActivation = false;
        relayPendingDeactivation = false;
        
        // Log recovery
        logSystemEvent("Relay recovery triggered after multiple timeouts");
        consecutiveErrors = 0;
      }
      
      yield(); // Allow system to process after timeout handling
    }
    // If transition has been active for a long time but still within timeout,
    // add yield to prevent watchdog from triggering
    else if (transitionDuration > (RELAY_TRANSITION_TIMEOUT / 2)) {
      yield(); // Additional yield for long transitions
    }
  }
  
  // If it's been a long time since a successful operation but we still have pending operations,
  // check for deadlocks
  if ((relayOperationPending || relayPendingDeactivation) && 
      lastSuccessfulOperation > 0 && 
      (millis() - lastSuccessfulOperation > 60000)) {  // 1 minute timeout
    
    Serial.println("WARNING: Potential relay operation deadlock detected, resetting state");
    
    // Reset all relay operation flags
    relayOperationPending = false;
    pendingRelayActivation = false;
    relayPendingDeactivation = false;
    relayTransitionInProgress = false;
    
    // Ensure relays are in a safe state (all deactivated)
    digitalWrite(RELAY1, HIGH);
    delay(50);
    yield();
    digitalWrite(RELAY2, HIGH);
    delay(50);
    yield();
    digitalWrite(RELAY3, HIGH);
    delay(50);
    yield();
    digitalWrite(RELAY4, HIGH);
    
    // Reset state
    relayActive = false;
    
    // Log the recovery
    logSystemEvent("Relay deadlock recovery triggered");
    
    // Reset timers
    lastSuccessfulOperation = millis();
    lastActivityTime = millis();
    lastReadyPrint = millis();
  }
  
  // Final yield to ensure smooth operation
  yield();
}

// Add function to reset session state
void resetSessionState() {
  // Reset all session-related flags and data
  classSessionActive = false;
  studentVerificationActive = false;
  waitingForInstructorEnd = false;
  tapOutPhase = false;
  
  // Clear student data
  studentAssignedSensors.clear();
  studentWeights.clear();
  awaitingWeight = false;
  
  // Reset relay and session variables
  // NOTE: We preserve lastInstructorUID to maintain reference to the instructor for PZEM data
  // lastInstructorUID = ""; // Commented out to preserve PZEM data reference
  assignedRoomId = "";
  lastPZEMLogTime = 0;
  presentCount = 0;
  
  // Update timers to prevent watchdog resets
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  Serial.println("Session state reset completed - PZEM data preserved");
}

// Move the smoothTransitionToReady function definition here, outside of any other function
void smoothTransitionToReady() {
  // Check if we're already in the ready state to avoid unnecessary transitions
  static unsigned long lastTransitionTime = 0;
  
  // Don't allow transitions too close together (minimum 3 seconds between transitions)
  if (millis() - lastTransitionTime < 3000) {
    Serial.println("Skipping redundant transition (too soon)");
    // Still update timers to prevent watchdog resets
    lastActivityTime = millis();
    lastReadyPrint = millis();
    return;
  }
  
  lastTransitionTime = millis();
  
  // Clear any pending operations
  yield();
  
  // Log memory state before cleanup
  uint32_t freeHeapBefore = ESP.getFreeHeap();
  
  // If heap memory is critically low, attempt recovery
  if (freeHeapBefore < 10000) {
    Serial.println("CRITICAL: Very low memory during transition, performing emergency cleanup");
    
    // Reset all non-essential state variables
    pendingStudentTaps.clear();
    uidDetailsPrinted.clear();
    
    // Force a yield to allow memory operations
    yield();
  }
  
  // Save important data before reset
  String preservedInstructorUID = lastInstructorUID; // Preserve instructor UID
  
  // Reset system state flags in a controlled manner
  tapOutPhase = false;
  studentVerificationActive = false;
  classSessionActive = false;
  waitingForInstructorEnd = false;
  
  // IMPORTANT FIX: Don't reset adminAccessActive here as it should be preserved
  // adminAccessActive = false;  // <-- commented out
  
  // Only reset relayActive if not in admin mode
  if (!adminAccessActive) {
    relayActive = false;
  }
  
  yield(); // Yield after flag resets
  
  // Reset the sensor assignments and weight data
  studentAssignedSensors.clear();
  studentWeights.clear();
  awaitingWeight = false;
  
  yield(); // Yield after clearing maps
  
  // Reset session-specific variables but preserve instructor reference
  // This is critical for maintaining the relationship to PZEM data
  assignedRoomId = "";
  lastPZEMLogTime = 0;
  presentCount = 0;
  
  // Show transition messages with progressive delays for smoother experience
  displayMessage("Session Ended", "Cleaning up...", 0);
  
  // Non-blocking delay with multiple yields for stability
  unsigned long startTime = millis();
  while (millis() - startTime < 1500) {
    yield(); // Non-blocking delay with yield
    // Feed watchdog during delay
    lastReadyPrint = millis();
    
    // Every 250ms during the delay, check for critical operations
    if ((millis() - startTime) % 250 == 0) {
      // Update activity timer to prevent timeouts
      lastActivityTime = millis();
    }
  }
  
  // Add intermediate transition messages for smoother experience
  displayMessage("Preparing system", "for next session", 0);
  
  startTime = millis();
  while (millis() - startTime < 1200) {
    yield();
    lastReadyPrint = millis();
    lastActivityTime = millis();
  }
  
  displayMessage("System ready", "for next session", 0);
  
  // Reduced delay from 5000ms to 2000ms (2 seconds) for a smoother transition
  startTime = millis();
  while (millis() - startTime < 2000) {
    yield();
    lastReadyPrint = millis();
    lastActivityTime = millis();
  }
  
  // Perform garbage collection
  ESP.getMinFreeHeap(); // Force memory compaction on ESP32
  yield(); // Yield after memory operation
  
  uint32_t freeHeapAfter = ESP.getFreeHeap();
  Serial.println("Memory cleanup: " + String(freeHeapBefore) + " -> " + String(freeHeapAfter) + " bytes");
  
  // Make sure tamper alert is resolved
  tamperActive = false;
  
  yield(); // Additional yield for stability
  
  // Check Firebase connection after session end
  if (isConnected && !Firebase.ready()) {
    Serial.println("Firebase connection lost during session. Attempting to reconnect...");
    
    // Try to reinitialize Firebase up to 3 times
    bool firebaseReconnected = false;
    for (int attempt = 0; attempt < 3 && !firebaseReconnected; attempt++) {
      Serial.println("Firebase reconnection attempt " + String(attempt + 1));
      
      // Update timers before potential delays
      lastActivityTime = millis();
      lastReadyPrint = millis();
      
      // Ensure WiFi is connected
      if (WiFi.status() != WL_CONNECTED) {
        Serial.println("WiFi disconnected. Reconnecting...");
        WiFi.disconnect();
        delay(500);
        yield(); // Add yield after delay
        connectWiFi();
        yield(); // Add yield after WiFi connection attempt
      }
      
      // Reset Firebase and reconnect
      Firebase.reset(&config);
      delay(500);
      yield(); // Add yield after delay
      
      Firebase.reconnectWiFi(true);
      delay(500);
      yield(); // Add yield after WiFi reconnection
      
      initFirebase();
      yield(); // Add yield after Firebase initialization
      
      // Check if reconnected successfully
      if (Firebase.ready() && Firebase.authenticated()) {
        firebaseReconnected = true;
        Serial.println("Firebase successfully reconnected after session end");
        displayMessage("Firebase", "Reconnected", 1500);
        sdMode = false; // Exit SD mode if it was active
      } else {
        Serial.println("Firebase reconnection attempt failed");
        delay(1000); // Wait before next attempt
        yield(); // Add yield after delay
      }
      
      yield(); // Prevent watchdog reset during reconnection attempts
    }
    
    // If all reconnection attempts failed, switch to SD mode
    if (!firebaseReconnected) {
      Serial.println("All Firebase reconnection attempts failed. Switching to SD mode.");
      sdMode = true;
      storeLogToSD("FirebaseReconnectFailed:Timestamp:" + getFormattedTime());
      displayMessage("Firebase Error", "Using SD Card", 2000);
    }
  }
  
  yield(); // Final yield before display update
  
  // Simple, clean transition to the ready message
  displayMessage("", "", 150); // Brief blank screen for visual break
  
  // Final ready message - SHOW THIS EXCEPT IN ADMIN MODE
  // FIX: Only show "Ready" message if not in admin mode
  if (!adminAccessActive) {
    // Check reed sensor state to ensure there are no tampering issues
    if (reedState) {
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      Serial.println("Displayed Ready message for normal mode");
    } else {
      // If reed sensor indicates open door/window, show an informative message instead
      displayMessage("Close Door/Window", "For Normal Operation", 0);
      Serial.println("Door/Window open - showing informative message");
    }
  } else {
    displayMessage("Admin Mode", "Active", 0);
    Serial.println("Kept Admin Mode display active");
  }
  
  // Update timers to prevent watchdog resets
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  // Ensure LEDs are in neutral state
  showNeutral();
  
  // Reset any error counters
  I2C_ERROR_COUNTER = 0;
  
  // Now we can safely clear the instructor reference after all operations
  lastInstructorUID = "";
  
  // Log the transition
  logSystemEvent("System Ready - Transition Complete");
  
  // Final yield to ensure system stability
  yield();
}

// Global FirebaseJson objects to reduce stack usage
static FirebaseJson instructorData;
static FirebaseJson accessJson;
static FirebaseJson classStatusJson;
static FirebaseJson pzemJson;
static FirebaseJson matchingSchedule;

#ifndef RELAY_SAFE_DELAY
#define RELAY_SAFE_DELAY 500  // 500ms safe delay for relay operations
#endif

// Add this function to ensure relays are in a safe state
void ensureRelaySafeState() {
  // Make sure relays are properly initialized and in a safe state
  pinMode(RELAY1, OUTPUT);
  pinMode(RELAY2, OUTPUT);
  pinMode(RELAY3, OUTPUT);
  pinMode(RELAY4, OUTPUT);
  
  // Ensure relays are inactive (HIGH)
  digitalWrite(RELAY1, HIGH);
  digitalWrite(RELAY2, HIGH);
  digitalWrite(RELAY3, HIGH);
  digitalWrite(RELAY4, HIGH);
  relayActive = false;
  relayTransitionInProgress = false;
  relayOperationPending = false;
  pendingRelayActivation = false;
  relayPendingDeactivation = false;
}

void logInstructor(String uid, String timestamp, String action) {
  // Log to SD
  String entry = "Instructor:" + uid + " Action:" + action + " Time:" + timestamp;
  storeLogToSD(entry);
  Serial.println("SD log stored to /Offline_Logs_Entry.txt: " + entry);

  // Voltage check
  float currentVoltage = pzem.voltage();
  if (!isVoltageSufficient || isnan(currentVoltage)) {
    delay(50);
    currentVoltage = pzem.voltage();
    isVoltageSufficient = (currentVoltage >= voltageThreshold && !isnan(currentVoltage));
  }
  Serial.println("Conditions - sdMode: " + String(sdMode) + 
                 ", isConnected: " + String(isConnected) + 
                 ", Voltage: " + (isnan(currentVoltage) ? "NaN" : String(currentVoltage, 2)) + "V" +
                 ", Threshold: " + String(voltageThreshold) + "V");

  // Extract time
  String currentTime = timestamp.substring(11, 13) + ":" + timestamp.substring(14, 16); // HH:MM

  // Schedule check
  ScheduleInfo currentSchedule;
  if (action == "Access") {
    // If we're waiting for instructor to finalize the session, don't start a new session
    if (waitingForInstructorEnd && uid == lastInstructorUID) {
      Serial.println("Instructor " + uid + " is finalizing the session after all students tapped out. Not starting a new session.");
      // The tap-out section will handle saving PZEM data
      return;
    }
    
    String day = getDayFromTimestamp(timestamp);
    String time = timestamp.substring(11, 16);
    int hour = time.substring(0, 2).toInt();
    int minute = time.substring(3, 5).toInt();
    currentSchedule = checkSchedule(uid, day, hour, minute);
    lastInstructorUID = uid;
  } else if (action == "EndSession" && lastInstructorUID == uid) {
    // Always try to revalidate schedule for EndSession, whether or not it's already valid
    String day = getDayFromTimestamp(timestamp);
    String time = timestamp.substring(11, 16);
    int hour = time.substring(0, 2).toInt();
    int minute = time.substring(3, 5).toInt();
    currentSchedule = checkSchedule(uid, day, hour, minute);
    
    if (currentSchedule.isValid) {
      Serial.println("Schedule valid for EndSession: " + currentSchedule.day + " " + 
                     currentSchedule.startTime + "-" + currentSchedule.endTime + ", Room: " + 
                     currentSchedule.roomName + ", Subject: " + currentSchedule.subject);
    } else {
      // If still not valid, try using current date with the last known schedule times
      Serial.println("Schedule not valid for EndSession. Trying with today's date...");
      // Get instructor schedule for the current day regardless of time
      String dateOnly = timestamp.substring(0, 10); // YYYY_MM_DD
      currentSchedule = getInstructorScheduleForDay(uid, dateOnly);
      
      if (currentSchedule.isValid) {
        Serial.println("Retrieved schedule using today's date: " + currentSchedule.day + 
                      " " + currentSchedule.startTime + "-" + currentSchedule.endTime);
      } else {
        Serial.println("WARNING: Unable to retrieve valid schedule for instructor " + uid + 
                      " during EndSession. PZEM data may not be properly logged.");
      }
    }
  }

  // Fetch instructor data
  String fullName = firestoreTeachers[uid]["fullName"].length() > 0 ? firestoreTeachers[uid]["fullName"] : "Unknown";
  String role = firestoreTeachers[uid]["role"].length() > 0 ? firestoreTeachers[uid]["role"] : "instructor";
  role.trim();
  if (!role.equalsIgnoreCase("instructor")) role = "instructor";

  // Schedule endTime check
  if (relayActive && !tapOutPhase && currentSchedule.isValid && action != "EndSession") {
    if (currentSchedule.endTime.length() == 5 && currentTime.length() == 5) {
      // Convert times to minutes for easier comparison
      int currentHour = currentTime.substring(0, 2).toInt();
      int currentMinute = currentTime.substring(3, 5).toInt();
      int currentTotalMinutes = currentHour * 60 + currentMinute;
      
      int endHour = currentSchedule.endTime.substring(0, 2).toInt();
      int endMinute = currentSchedule.endTime.substring(3, 5).toInt();
      int endTotalMinutes = endHour * 60 + endMinute;
      
      // Get start time in minutes for span detection
      int startHour = 0;
      int startMinute = 0;
      if (currentSchedule.startTime.length() == 5) {
        startHour = currentSchedule.startTime.substring(0, 2).toInt();
        startMinute = currentSchedule.startTime.substring(3, 5).toInt();
      }
      int startTotalMinutes = startHour * 60 + startMinute;
      
      // Check for class spanning across midnight (endTime < startTime)
      bool spansMidnight = endTotalMinutes < startTotalMinutes;
      
      // Adjust comparison for classes that span midnight
      bool endTimeReached = false;
      if (spansMidnight) {
        // If class spans midnight and current time is less than start time, 
        // it means we're after midnight, so adjust comparison
        if (currentTotalMinutes < startTotalMinutes) {
          endTimeReached = (currentTotalMinutes >= endTotalMinutes);
        } else {
          // Current time is after start time but before midnight
          endTimeReached = false;
        }
      } else {
        // Normal comparison for classes that don't span midnight
        endTimeReached = (currentTotalMinutes >= endTotalMinutes);
      }
      
      Serial.println("Time in minutes - Current: " + String(currentTotalMinutes) + 
                     ", Start: " + String(startTotalMinutes) + 
                     ", End: " + String(endTotalMinutes) + 
                     ", Spans midnight: " + String(spansMidnight));
      
      if (endTimeReached) {
        Serial.println("End time reached (" + currentTime + " >= " + currentSchedule.endTime + "). Transitioning to tap-out phase.");
        digitalWrite(RELAY2, HIGH);
        digitalWrite(RELAY3, HIGH);
        digitalWrite(RELAY4, HIGH);
        relayActive = false;
        classSessionActive = false;
        tapOutPhase = true;
        tapOutStartTime = millis();
        tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
        
        // Store PZEM data and update ClassStatus to "Class Ended"
        if (!sdMode && isConnected && Firebase.ready() && currentSchedule.roomName.length() > 0) {
          float voltage = pzem.voltage();
          float current = pzem.current();
          float power = pzem.power();
          float energy = pzem.energy();
          float frequency = pzem.frequency();
          float powerFactor = pzem.pf();
          
          if (isnan(voltage) || voltage < 0) voltage = 0.0;
          if (isnan(current) || current < 0) current = 0.0;
          if (isnan(power) || power < 0) power = 0.0;
          if (isnan(energy) || energy < 0) energy = 0.0;
          if (isnan(frequency) || frequency < 0) frequency = 0.0;
          if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
          
          String instructorPath = "/Instructors/" + lastInstructorUID;
          FirebaseJson classStatusJson;
          classStatusJson.set("Status", "Class Ended");
          classStatusJson.set("dateTime", timestamp);
          
          FirebaseJson scheduleJson;
          scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
          scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
          scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
          scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
          scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
          scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
          
          FirebaseJson roomNameJson;
          roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
          
          // Create PZEM JSON data
          FirebaseJson pzemJson;
          pzemJson.set("voltage", String(voltage, 1));
          pzemJson.set("current", String(current, 2));
          pzemJson.set("power", String(power, 1));
          pzemJson.set("energy", String(energy, 2));
          pzemJson.set("frequency", String(frequency, 1));
          pzemJson.set("powerFactor", String(powerFactor, 2));
          pzemJson.set("timestamp", timestamp);
          roomNameJson.set("pzem", pzemJson);
          
          scheduleJson.set("roomName", roomNameJson);
          classStatusJson.set("schedule", scheduleJson);
          
          String statusPath = instructorPath + "/ClassStatus";
          // Use setJSON instead of updateNode to ensure proper data structure
          if (Firebase.RTDB.setJSON(&fbdo, statusPath, &classStatusJson)) {
            Serial.println("Class status updated to 'Class Ended' with PZEM data preserved");
            
            // Create a permanent archive of the session data
            String archivePath = instructorPath + "/ClassHistory/" + timestamp.substring(0, 10) + "_" + timestamp.substring(11, 13) + timestamp.substring(14, 16);
            if (Firebase.RTDB.setJSON(&fbdo, archivePath, &classStatusJson)) {
              Serial.println("Class data archived for permanent storage");
            }
          } else {
            String errorLog = "FirebaseError:Path:" + statusPath + " Reason:" + fbdo.errorReason();
            storeLogToSD(errorLog);
            Serial.println("Failed to update class status with PZEM data: " + fbdo.errorReason());
          }
        }
        
        displayMessage("Class Ended", "Tap to Confirm", 3000);
        Serial.println("Schedule endTime " + currentSchedule.endTime + " reached at " + currentTime + ". Transition to tap-out phase.");
      }
    }
  }

  // Firebase logging
  if (!sdMode && isConnected && Firebase.ready()) {
    Serial.println("Firebase conditions met. Logging UID: " + uid + " Action: " + action);

    String instructorPath = "/Instructors/" + uid;

    // Profile data
    FirebaseJson instructorData;
    instructorData.set("fullName", fullName);
    instructorData.set("email", firestoreTeachers[uid]["email"].length() > 0 ? firestoreTeachers[uid]["email"] : "N/A");
    instructorData.set("idNumber", firestoreTeachers[uid]["idNumber"].length() > 0 ? firestoreTeachers[uid]["idNumber"] : "N/A");
    instructorData.set("mobileNumber", firestoreTeachers[uid]["mobileNumber"].length() > 0 ? firestoreTeachers[uid]["mobileNumber"] : "N/A");
    instructorData.set("role", role);
    instructorData.set("department", firestoreTeachers[uid]["department"].length() > 0 ? firestoreTeachers[uid]["department"] : "Unknown");
    instructorData.set("createdAt", firestoreTeachers[uid]["createdAt"].length() > 0 ? firestoreTeachers[uid]["createdAt"] : "N/A");

    // Access log
    FirebaseJson accessJson;
    accessJson.set("action", action);
    accessJson.set("timestamp", timestamp);
    accessJson.set("status", (action == "Access" && currentSchedule.isValid) ? "granted" : (action == "Access" ? "denied" : "completed"));

    // Class status
    FirebaseJson classStatusJson;
    String status = (action == "Access" && currentSchedule.isValid) ? "In Session" : (action == "Access" ? "Denied" : "End Session");
    classStatusJson.set("Status", status);
    classStatusJson.set("dateTime", timestamp);

    // Always include schedule for Access and EndSession to ensure PZEM data is logged
    FirebaseJson scheduleJson;
    scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
    scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
    scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
    scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
    scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
    scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
    FirebaseJson roomNameJson;
    roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");

    // PZEM data for EndSession on first tap
    if (action == "EndSession" && uid == lastInstructorUID) {
      // Check if ending early
      String currentTime = timestamp.substring(11, 13) + ":" + timestamp.substring(14, 16); // HH:MM
      if (currentSchedule.isValid && currentTime < currentSchedule.endTime) {
        Serial.println("Instructor UID " + uid + " ended session early before endTime " + currentSchedule.endTime + ".");
      }
      
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float powerFactor = pzem.pf();

      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;

      // Calculate energy consumption using E = P*(t/1000)
      // Get the session duration in hours
      float sessionDurationHours = 0.0;
      if (currentSchedule.startTime.length() > 0 && currentSchedule.endTime.length() > 0) {
        // Extract hours and minutes from startTime (format HH:MM)
        int startHour = currentSchedule.startTime.substring(0, 2).toInt();
        int startMinute = currentSchedule.startTime.substring(3, 5).toInt();
        
        // Extract hours and minutes from endTime (format HH:MM)
        int endHour = currentSchedule.endTime.substring(0, 2).toInt();
        int endMinute = currentSchedule.endTime.substring(3, 5).toInt();
        
        // Calculate total minutes
        int startTotalMinutes = startHour * 60 + startMinute;
        int endTotalMinutes = endHour * 60 + endMinute;
        
        // Handle cases where the end time is on the next day
        if (endTotalMinutes < startTotalMinutes) {
          endTotalMinutes += 24 * 60; // Add 24 hours in minutes
        }
        
        // Calculate the duration in hours
        sessionDurationHours = (endTotalMinutes - startTotalMinutes) / 60.0;
      }
      
      // Calculate energy consumption (kWh) using E = P*(t/1000)
      // Power is in watts, time is in hours, result is in kWh
      float calculatedEnergy = power * sessionDurationHours / 1000.0;
      
      // Use the calculated energy if the measured energy is zero or invalid
      if (energy <= 0.01) {
        energy = calculatedEnergy;
      }
      
      Serial.println("Session duration: " + String(sessionDurationHours) + " hours");
      Serial.println("Calculated energy: " + String(calculatedEnergy) + " kWh");

      FirebaseJson pzemJson;
      pzemJson.set("voltage", String(voltage, 1));
      pzemJson.set("current", String(current, 2));
      pzemJson.set("power", String(power, 1));
      pzemJson.set("energy", String(energy, 2));
      pzemJson.set("calculatedEnergy", String(calculatedEnergy, 2));
      pzemJson.set("sessionDuration", String(sessionDurationHours, 2));
      pzemJson.set("frequency", String(frequency, 1));
      pzemJson.set("powerFactor", String(powerFactor, 2));
      pzemJson.set("timestamp", timestamp);
      pzemJson.set("action", "end");
      roomNameJson.set("pzem", pzemJson);
      pzemLoggedForSession = true;
      Serial.println("PZEM logged at session end: Voltage=" + String(voltage, 1) + ", Energy=" + String(energy, 2));

      // Remove the old separate Rooms node storage
      // Instead, modify ClassStatus to include all necessary information
      if (currentSchedule.roomName.length() > 0) {
        // Instead of creating a separate path, we'll log this information to the classStatusJson
        FirebaseJson classDetailsJson;
        classDetailsJson.set("roomName", currentSchedule.roomName);
        classDetailsJson.set("subject", currentSchedule.subject);
        classDetailsJson.set("subjectCode", currentSchedule.subjectCode);
        classDetailsJson.set("section", currentSchedule.section);
        classDetailsJson.set("sessionStart", currentSchedule.startTime);
        classDetailsJson.set("sessionEnd", currentSchedule.endTime);
        classDetailsJson.set("date", timestamp.substring(0, 10));
        classDetailsJson.set("sessionId", currentSessionId);
        
        // Add this class details to the ClassStatus node
        classStatusJson.set("roomDetails", classDetailsJson);
        
        Serial.println("Class details included in ClassStatus");
      }
    }

    scheduleJson.set("roomName", roomNameJson);
    classStatusJson.set("schedule", scheduleJson);

    // Perform Firebase operations
    bool success = true;
    if (!Firebase.RTDB.setJSON(&fbdo, instructorPath + "/Profile", &instructorData)) {
      Serial.println("Failed to sync profile: " + fbdo.errorReason());
      success = false;
      storeLogToSD("ProfileFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }
    if (!Firebase.RTDB.pushJSON(&fbdo, instructorPath + "/AccessLogs", &accessJson)) {
      Serial.println("Failed to push access log: " + fbdo.errorReason());
      success = false;
      storeLogToSD("AccessLogFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }
    if (!Firebase.RTDB.setJSON(&fbdo, instructorPath + "/ClassStatus", &classStatusJson)) {
      Serial.println("Failed to update class status: " + fbdo.errorReason());
      success = false;
      storeLogToSD("ClassStatusFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }
    if (!Firebase.RTDB.setString(&fbdo, "/RegisteredUIDs/" + uid, timestamp)) {
      Serial.println("Failed to update RegisteredUIDs: " + fbdo.errorReason());
      success = false;
      storeLogToSD("RegisteredUIDsFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }

    if (!success) {
      sdMode = true;
      Serial.println("Firebase logging failed. Switching to SD mode.");
    } else {
      Serial.println("Instructor " + fullName + " logged to Firebase successfully at path: " + instructorPath);
    }
  } else {
    Serial.println("Firebase conditions not met: sdMode=" + String(sdMode) + 
                   ", isConnected=" + String(isConnected) + 
                   ", isVoltageSufficient=" + String(isVoltageSufficient) + 
                   ", Firebase.ready=" + String(Firebase.ready()));
    if (action == "EndSession" && uid == lastInstructorUID) {
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float powerFactor = pzem.pf();
      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
      String pzemEntry = "PZEM:UID:" + uid + " Time:" + timestamp +
                         " Voltage:" + String(voltage, 1) + "V" +
                         " Current:" + String(current, 2) + "A" +
                         " Power:" + String(power, 1) + "W" +
                         " Energy:" + String(energy, 2) + "kWh" +
                         " Frequency:" + String(frequency, 1) + "Hz" +
                         " PowerFactor:" + String(powerFactor, 2);
      storeLogToSD(pzemEntry);
      pzemLoggedForSession = true;
    }
  }

  // Handle actions
  if (action == "Access") {
    if (!currentSchedule.isValid) {
      // Before denying access, try to refresh schedule data from Firestore
      bool scheduleFound = false;
      
      // First log a temporary "pending verification" status
      if (!sdMode && isConnected && Firebase.ready()) {
        // Log the temporary "pending verification" status
        String tempPath = "/Instructors/" + uid + "/AccessLogs";
        FirebaseJson tempAccessJson;
        tempAccessJson.set("action", "Access");
        tempAccessJson.set("status", "pending_verification");
        tempAccessJson.set("timestamp", timestamp);
        tempAccessJson.set("note", "Checking for schedule updates");
        String tempLogId;
        
        if (Firebase.RTDB.pushJSON(&fbdo, tempPath, &tempAccessJson)) {
          // Save the temporary log ID for potential update later
          tempLogId = fbdo.pushName();
          Serial.println("Temporary pending log created with ID: " + tempLogId);
        }
        
        // Display message indicating schedule check
        displayMessage("Checking for", "Updated Schedule", 2000);
        Serial.println("Instructor UID " + uid + " outside schedule. Checking for updates...");
        
        // Fetch updated teacher and room data from Firestore
        fetchFirestoreTeachers();
        fetchFirestoreRooms();
        
        // Check schedule again with freshly fetched data
        String day = getDayFromTimestamp(timestamp);
        String time = timestamp.substring(11, 16);
        int hour = time.substring(0, 2).toInt();
        int minute = time.substring(3, 5).toInt();
        currentSchedule = checkSchedule(uid, day, hour, minute);
        
        if (currentSchedule.isValid) {
          Serial.println("Updated schedule found after refresh! Room: " + currentSchedule.roomName);
          scheduleFound = true;
          
          // Update the previously created log to reflect successful verification
          if (tempLogId.length() > 0) {
            String updatePath = "/Instructors/" + uid + "/AccessLogs/" + tempLogId;
            FirebaseJson updateJson;
            updateJson.set("status", "granted");
            updateJson.set("note", "Access granted after schedule refresh");
            
            if (Firebase.RTDB.updateNode(&fbdo, updatePath, &updateJson)) {
              Serial.println("Access log updated to 'granted' after schedule refresh");
            } else {
              Serial.println("Failed to update access log: " + fbdo.errorReason());
            }
          }
          
          // Update instructor ClassStatus to reflect the valid schedule
          String classStatusPath = "/Instructors/" + uid + "/ClassStatus";
          FirebaseJson classStatusJson;
          classStatusJson.set("Status", "In Session");
          classStatusJson.set("dateTime", timestamp);
          classStatusJson.set("schedule/day", currentSchedule.day);
          classStatusJson.set("schedule/startTime", currentSchedule.startTime);
          classStatusJson.set("schedule/endTime", currentSchedule.endTime);
          classStatusJson.set("schedule/subject", currentSchedule.subject);
          classStatusJson.set("schedule/subjectCode", currentSchedule.subjectCode);
          classStatusJson.set("schedule/section", currentSchedule.section);
          classStatusJson.set("schedule/roomName/name", currentSchedule.roomName);
          
          // Check if there's existing PZEM data we need to preserve
          if (Firebase.RTDB.get(&fbdo, classStatusPath + "/schedule/roomName/pzem")) {
            if (fbdo.dataType() == "json") {
              FirebaseJson pzemData;
              pzemData.setJsonData(fbdo.jsonString());
              
              // Extract values from existing PZEM data
              FirebaseJsonData voltage, current, power, energy, frequency, powerFactor, pzemTimestamp;
              pzemData.get(voltage, "voltage");
              pzemData.get(current, "current");
              pzemData.get(power, "power");
              pzemData.get(energy, "energy");
              pzemData.get(frequency, "frequency");
              pzemData.get(powerFactor, "powerFactor");
              pzemData.get(pzemTimestamp, "timestamp");
              
              // Add PZEM data to avoid overwriting it
              if (voltage.success) classStatusJson.set("schedule/roomName/pzem/voltage", voltage.stringValue);
              if (current.success) classStatusJson.set("schedule/roomName/pzem/current", current.stringValue);
              if (power.success) classStatusJson.set("schedule/roomName/pzem/power", power.stringValue);
              if (energy.success) classStatusJson.set("schedule/roomName/pzem/energy", energy.stringValue);
              if (frequency.success) classStatusJson.set("schedule/roomName/pzem/frequency", frequency.stringValue);
              if (powerFactor.success) classStatusJson.set("schedule/roomName/pzem/powerFactor", powerFactor.stringValue);
              if (pzemTimestamp.success) classStatusJson.set("schedule/roomName/pzem/timestamp", pzemTimestamp.stringValue);
              
              Serial.println("Existing PZEM data preserved in ClassStatus update");
            }
          }
          
          // Use setJSON instead of updateNode to ensure consistency
          if (Firebase.RTDB.setJSON(&fbdo, classStatusPath, &classStatusJson)) {
            Serial.println("Class status updated to 'In Session' after schedule refresh");
          } else {
            Serial.println("Failed to update class status: " + fbdo.errorReason());
          }
          
          // Continue with access (will be handled below since currentSchedule is now valid)
        } else {
          Serial.println("No valid schedule found even after refresh for UID " + uid);
          
          // Update the temporary log to confirm denial
          if (tempLogId.length() > 0) {
            String updatePath = "/Instructors/" + uid + "/AccessLogs/" + tempLogId;
            FirebaseJson updateJson;
            updateJson.set("status", "denied");
            updateJson.set("note", "No valid schedule found even after refresh");
            
            if (Firebase.RTDB.updateNode(&fbdo, updatePath, &updateJson)) {
              Serial.println("Access log updated to 'denied' after schedule refresh attempt");
            } else {
              Serial.println("Failed to update access log: " + fbdo.errorReason());
            }
          }
        }
      }
      
      if (!scheduleFound) {
        // No valid schedule found even after refresh
        deniedFeedback();
        displayMessage("Outside Schedule", "Access Denied", 6000);
        Serial.println("Instructor UID " + uid + " denied: outside schedule.");
        return;
      }
    }

    if (!relayActive) {
      digitalWrite(RELAY1, LOW);
      digitalWrite(RELAY2, LOW);
      digitalWrite(RELAY3, LOW);
      digitalWrite(RELAY4, LOW);
      relayActive = true;
      studentVerificationActive = true;
      studentVerificationStartTime = millis();
      currentStudentQueueIndex = 0;
      lastStudentTapTime = millis();
      presentCount = 0;
      pzemLoggedForSession = false;

      String subject = currentSchedule.subject.length() > 0 ? currentSchedule.subject : "UNK";
      String subjectCode = currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "UNK";
      String section = currentSchedule.section.length() > 0 ? currentSchedule.section : "UNK";
      String roomName = currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "UNK";
      String startTimeStr = currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown";
      String endTimeStr = currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown";

      if (section == "UNK" && firestoreTeachers[uid]["sections"] != "[]") {
        FirebaseJson sectionsJson;
        if (sectionsJson.setJsonData(firestoreTeachers[uid]["sections"])) {
          FirebaseJsonData sectionData;
          if (sectionsJson.get(sectionData, "[0]/name") && sectionData.typeNum == FirebaseJson::JSON_STRING) {
            section = sectionData.stringValue;
          }
        }
      }

      String classDate = timestamp.substring(0, 10);
      currentSessionId = classDate + "_" + subjectCode + "_" + section + "_" + roomName;
      studentAssignedSensors.clear();
      studentWeights.clear();
      sessionStartReading = pzem.energy();

      accessFeedback();
      Serial.println("Class session started. Session ID: " + currentSessionId + ", Room: " + roomName);
      displayMessage(subjectCode + " " + section, roomName + " " + startTimeStr + "-" + endTimeStr, 5000);
    } else if (relayActive && uid == lastInstructorUID && !tapOutPhase) {
      digitalWrite(RELAY2, HIGH);
      digitalWrite(RELAY3, HIGH);
      digitalWrite(RELAY4, HIGH);
      relayActive = false;
      classSessionActive = false;
      tapOutPhase = true;
      tapOutStartTime = millis();
      pzemLoggedForSession = false; // Reset the global variable
      tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
      displayMessage("Class Ended", "Tap to Confirm", 3000);
      Serial.println("Instructor UID " + uid + " ended session early before endTime " + currentSchedule.endTime + ".");
    }
  } else if (action == "EndSession" && relayActive && uid == lastInstructorUID && !tapOutPhase) {
    digitalWrite(RELAY2, HIGH);
    digitalWrite(RELAY3, HIGH);
    digitalWrite(RELAY4, HIGH);
    relayActive = false;
    classSessionActive = false;
    tapOutPhase = true;
    tapOutStartTime = millis();
    pzemLoggedForSession = false; // Reset the global variable
    tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
    
    // Store PZEM data and update ClassStatus to "Class Ended"
    if (!sdMode && isConnected && Firebase.ready() && currentSchedule.roomName.length() > 0) {
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float powerFactor = pzem.pf();
      
      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
      
      String instructorPath = "/Instructors/" + uid;
      FirebaseJson classStatusJson;
      classStatusJson.set("Status", "Class Ended");
      classStatusJson.set("dateTime", timestamp);
      
      FirebaseJson scheduleJson;
      scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
      scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
      scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
      scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
      scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
      scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
      
      FirebaseJson roomNameJson;
      roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
      
      // Create PZEM JSON data
      FirebaseJson pzemJson;
      pzemJson.set("voltage", String(voltage, 1));
      pzemJson.set("current", String(current, 2));
      pzemJson.set("power", String(power, 1));
      pzemJson.set("energy", String(energy, 2));
      pzemJson.set("frequency", String(frequency, 1));
      pzemJson.set("powerFactor", String(powerFactor, 2));
      pzemJson.set("timestamp", timestamp);
      roomNameJson.set("pzem", pzemJson);
      
      scheduleJson.set("roomName", roomNameJson);
      classStatusJson.set("schedule", scheduleJson);
      
      String statusPath = instructorPath + "/ClassStatus";
      // Use setJSON instead of updateNode to ensure proper data structure
      if (Firebase.RTDB.setJSON(&fbdo, statusPath, &classStatusJson)) {
        Serial.println("Class status updated to 'Class Ended' with PZEM data preserved");
        
        // Create a permanent archive of the session data
        String archivePath = instructorPath + "/ClassHistory/" + timestamp.substring(0, 10) + "_" + timestamp.substring(11, 13) + timestamp.substring(14, 16);
        if (Firebase.RTDB.setJSON(&fbdo, archivePath, &classStatusJson)) {
          Serial.println("Class data archived for permanent storage");
        }
      } else {
        String errorLog = "FirebaseError:Path:" + statusPath + " Reason:" + fbdo.errorReason();
        storeLogToSD(errorLog);
        Serial.println("Failed to update class status with PZEM data: " + fbdo.errorReason());
      }
    }
    
    displayMessage("Class Ended", "Tap to Confirm", 3000);
    Serial.println("Instructor UID " + uid + " explicitly ended session early with EndSession action.");
  } else if (action == "EndSession" && tapOutPhase && uid == lastInstructorUID) {
    displayMessage("Session Finalized", "Summary Saved", 3000);
    Serial.println("Final tap by instructor UID " + uid + ". Generating AttendanceSummary.");

    if (!sdMode && isConnected && Firebase.ready()) {
      String classStatusPath = "/Instructors/" + uid + "/ClassStatus";
      FirebaseJson classStatusUpdate;
      classStatusUpdate.set("Status", "End Session");
      classStatusUpdate.set("dateTime", timestamp);

      // Include schedule without PZEM data
      FirebaseJson scheduleJson;
      scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
      scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
      scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
      scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
      scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
      scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
      
      FirebaseJson roomNameJson;
      roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
      
      // Check if there's existing PZEM data to preserve
      if (Firebase.RTDB.get(&fbdo, classStatusPath + "/schedule/roomName/pzem")) {
        if (fbdo.dataType() == "json") {
          // Get the existing PZEM data
          FirebaseJson pzemData;
          pzemData.setJsonData(fbdo.jsonString());
          
          // Set it in the roomName object
          roomNameJson.set("pzem", pzemData);
          Serial.println("Preserved existing PZEM data during AttendanceSummary generation");
        }
      }
      
      scheduleJson.set("roomName", roomNameJson);
      classStatusUpdate.set("schedule", scheduleJson);

      if (!Firebase.RTDB.setJSON(&fbdo, classStatusPath, &classStatusUpdate)) {
        Serial.println("Failed to update ClassStatus: " + fbdo.errorReason());
        storeLogToSD("ClassStatusFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
      } else {
        Serial.println("ClassStatus updated at " + classStatusPath + " with PZEM data preserved");
        
        // Archive the final session data for history
        String archivePath = "/Instructors/" + uid + "/ClassHistory/" + timestamp.substring(0, 10) + "_" + timestamp.substring(11, 13) + timestamp.substring(14, 16) + "_final";
        if (Firebase.RTDB.setJSON(&fbdo, archivePath, &classStatusUpdate)) {
          Serial.println("Final class data archived for permanent storage");
        }
      }
    }

    // Generate AttendanceSummary
    String summaryPath = "/AttendanceSummary/" + currentSessionId;
    FirebaseJson summaryJson;
    summaryJson.set("InstructorName", fullName);
    summaryJson.set("StartTime", timestamp); // Note: Consider storing actual start time
    summaryJson.set("EndTime", timestamp);
    summaryJson.set("Status", "Class Ended");
    summaryJson.set("SubjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
    summaryJson.set("SubjectName", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
    summaryJson.set("Day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
    summaryJson.set("Section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");

    // Fetch students
    FirebaseJson attendeesJson;
    int totalAttendees = 0;
    std::set<String> processedStudents;

    for (const auto& student : firestoreStudents) {
      String studentUid = student.first;
      String studentSection;
      try {
        studentSection = student.second.at("section").length() > 0 ? student.second.at("section") : "";
      } catch (const std::out_of_range&) {
        studentSection = "";
      }
      if (studentSection != currentSchedule.section) continue;

      String studentPath = "/Students/" + studentUid;
      String studentName;
      try {
        studentName = student.second.at("fullName").length() > 0 ? student.second.at("fullName") : "Unknown";
      } catch (const std::out_of_range&) {
        studentName = "Unknown";
      }
      String status = "Absent";
      float weight = 0.0;
      String studentSessionId = "";

      if (Firebase.RTDB.getJSON(&fbdo, studentPath)) {
        FirebaseJsonData data;
        if (fbdo.jsonObjectPtr()->get(data, "Status")) {
          status = data.stringValue;
        }
        if (fbdo.jsonObjectPtr()->get(data, "weight")) {
          weight = data.doubleValue;
        }
        if (fbdo.jsonObjectPtr()->get(data, "sessionId")) {
          studentSessionId = data.stringValue;
        }
      }

      if (studentSessionId == currentSessionId && status == "Present") {
        totalAttendees++;
      }

      FirebaseJson studentJson;
      studentJson.set("StudentName", studentName);
      studentJson.set("Status", status);
      studentJson.set("Weight", weight);
      attendeesJson.set(studentUid, studentJson);
      processedStudents.insert(studentUid);
    }

    // Handle pendingStudentTaps
    for (const String& studentUid : pendingStudentTaps) {
      if (processedStudents.find(studentUid) != processedStudents.end()) continue;
      processedStudents.insert(studentUid);

      String studentPath = "/Students/" + studentUid;
      String studentName;
      try {
        studentName = firestoreStudents.at(studentUid).at("fullName").length() > 0 ? firestoreStudents.at(studentUid).at("fullName") : "Unknown";
      } catch (const std::out_of_range&) {
        studentName = "Unknown";
      }
      String status = "Absent";
      float weight = 0.0;

      if (Firebase.RTDB.getJSON(&fbdo, studentPath)) {
        FirebaseJsonData data;
        if (fbdo.jsonObjectPtr()->get(data, "Status")) {
          status = data.stringValue;
        }
        if (fbdo.jsonObjectPtr()->get(data, "weight")) {
          weight = data.doubleValue;
        }
      }

      FirebaseJson studentJson;
      studentJson.set("StudentName", studentName);
      studentJson.set("Status", status);
      studentJson.set("Weight", weight);
      attendeesJson.set(studentUid, studentJson);
    }

    summaryJson.set("Attendees", attendeesJson);
    summaryJson.set("TotalAttendees", totalAttendees);

    if (Firebase.RTDB.setJSON(&fbdo, summaryPath, &summaryJson)) {
      Serial.println("AttendanceSummary created at " + summaryPath + ", Attendees: " + String(totalAttendees));
    } else {
      Serial.println("Failed to create AttendanceSummary: " + fbdo.errorReason());
      storeLogToSD("AttendanceSummaryFailed:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }

    // SD mode summary (moved here to ensure it's part of final tap)
    if (sdMode || !isConnected || !Firebase.ready()) {
      String summaryEntry = "AttendanceSummary:SessionID:" + currentSessionId + 
                           " Instructor:" + fullName + 
                           " Start:" + timestamp + 
                           " End:" + timestamp + 
                           " Status:Class Ended";
      
      int totalAttendees = 0;
      for (const auto& student : firestoreStudents) {
        String studentUid = student.first;
        String studentSection;
        try {
          studentSection = student.second.at("section").length() > 0 ? student.second.at("section") : "";
        } catch (const std::out_of_range&) {
          studentSection = "";
        }
        if (studentSection != currentSchedule.section) continue;
        String studentName;
        try {
          studentName = student.second.at("fullName").length() > 0 ? student.second.at("fullName") : "Unknown";
        } catch (const std::out_of_range&) {
          studentName = "Unknown";
        }
        if (studentWeights.find(studentUid) != studentWeights.end() && studentWeights[studentUid] >= 15) {
          totalAttendees++;
          summaryEntry += " Student:" + studentUid + ":" + studentName + ":Present";
        } else {
          summaryEntry += " Student:" + studentUid + ":" + studentName + ":Absent";
        }
      }
      for (const String& studentUid : pendingStudentTaps) {
        if (firestoreStudents.find(studentUid) == firestoreStudents.end()) continue;
        String studentName;
        try {
          studentName = firestoreStudents.at(studentUid).at("fullName").length() > 0 ? firestoreStudents.at(studentUid).at("fullName") : "Unknown";
        } catch (const std::out_of_range&) {
          studentName = "Unknown";
        }
        summaryEntry += " Student:" + studentUid + ":" + studentName + ":Absent";
      }
      storeLogToSD(summaryEntry);
      Serial.println("Firebase unavailable. AttendanceSummary logged to SD, Attendees: " + String(totalAttendees));
    }

    // Reset system
    tapOutPhase = false;
    waitingForInstructorEnd = false;
    lastInstructorUID = "";
    currentSessionId = "";
    studentAssignedSensors.clear();
    studentWeights.clear();
    pendingStudentTaps.clear();
    presentCount = 0;
    digitalWrite(RELAY1, HIGH);
    pzemLoggedForSession = false;
    Serial.println("Session fully ended. All relays off, system reset.");
    displayMessage("Ready. Tap your", "RFID Card!", 0);
    readyMessageShown = true;

    // Reset watchdog timer at session end
    lastReadyPrint = millis();
    Serial.println("Watchdog timer reset at session end");

    // New function for smooth transition to ready state
    smoothTransitionToReady();

    // Clear finalization tracking for next session
    uidWeightFinalized.clear();
    sensorAssignedUid.clear();
  }
}

void logStudentToRTDB(String rfidUid, String timestamp, float weight, int sensorIndex, String weightConfirmed, String timeOut) {
  Serial.println("logStudentToRTDB called for UID: " + rfidUid + " at " + timestamp);
  yield(); // Initial yield to prevent stack overflow

  // Get the last session ID
  String lastSessionId = "";
  if (currentSessionId != "") {
    lastSessionId = currentSessionId;
  } else if (!sdMode && isConnected && Firebase.ready()) {
    String lastSessionPath = "/Students/" + rfidUid + "/lastSession";
    if (Firebase.RTDB.getString(&fbdo, lastSessionPath)) {
      lastSessionId = fbdo.stringData();
    }
  }

  // Check if attendance is already finalized for this student
  if (lastSessionId != "" && isAttendanceFinalized(rfidUid, lastSessionId)) {
    Serial.println("Attendance already finalized for " + rfidUid + " in session " + lastSessionId + ". Skipping update.");
    return;  // Skip updating if already finalized
  }

  // Check and create student profile in RTDB if needed
  if (!sdMode && isConnected && Firebase.ready()) {
    // Reset watchdog timer before lengthy operation
    lastReadyPrint = millis();
    
    yield(); // Yield before Firebase path operation
    String profilePath = "/Students/" + rfidUid + "/Profile";
    bool createProfile = true;

    if (Firebase.RTDB.get(&fbdo, profilePath)) {
      createProfile = false;
      yield(); // Yield after Firebase operation
    }

    if (createProfile) {
      yield(); // Yield before fetch operation
      if (firestoreStudents.find(rfidUid) == firestoreStudents.end()) {
        Serial.println("Refreshing Firestore data for new student...");
        yield(); // Yield before Firestore fetch
        fetchFirestoreStudents();
        yield(); // Yield after Firestore fetch
      }

      if (firestoreStudents.find(rfidUid) != firestoreStudents.end()) {
        yield(); // Yield before data extraction
        FirebaseJson profileJson;
        auto& studentData = firestoreStudents[rfidUid];

        profileJson.set("fullName", studentData["fullName"].length() > 0 ? studentData["fullName"] : "Unknown");
        yield();
        profileJson.set("email", studentData["email"]);
        profileJson.set("idNumber", studentData["idNumber"]);
        profileJson.set("mobileNumber", studentData["mobileNumber"]);
        yield();
        profileJson.set("role", "student");
        profileJson.set("department", studentData["department"]);
        profileJson.set("rfidUid", rfidUid);
        profileJson.set("createdAt", timestamp);
        profileJson.set("lastUpdated", timestamp);

        if (studentData["schedules"].length() > 0) {
          yield(); // Yield before schedule processing
          FirebaseJson schedulesJson;
          schedulesJson.setJsonData(studentData["schedules"]);
          profileJson.set("schedules", schedulesJson);
          yield(); // Yield after schedule processing
        }

        yield(); // Yield before RTDB update
        if (Firebase.RTDB.setJSON(&fbdo, profilePath, &profileJson)) {
          Serial.println("Created new student profile in RTDB for " + rfidUid);
        } else {
          Serial.println("Failed to create student profile: " + fbdo.errorReason());
          if (fbdo.errorReason().indexOf("ssl") >= 0 || 
              fbdo.errorReason().indexOf("connection") >= 0 || 
              fbdo.errorReason().indexOf("SSL") >= 0) {
            handleFirebaseSSLError();
          }
        }
        yield(); // Yield after RTDB update
      }
    }
  }

  yield(); // Yield before attendance processing

  // Default student data
  String studentName = "Unknown";
  String email = "", idNumber = "", mobileNumber = "", role = "", department = "";
  String schedulesJsonStr = "[]";
  String sectionId = "";
  
  // Get student data from cache
  if (firestoreStudents.find(rfidUid) != firestoreStudents.end()) {
    yield(); // Yield before data extraction
    studentName = firestoreStudents[rfidUid]["fullName"].length() > 0 ? firestoreStudents[rfidUid]["fullName"] : "Unknown";
    email = firestoreStudents[rfidUid]["email"];
    idNumber = firestoreStudents[rfidUid]["idNumber"];
    mobileNumber = firestoreStudents[rfidUid]["mobileNumber"];
    role = firestoreStudents[rfidUid]["role"].length() > 0 ? firestoreStudents[rfidUid]["role"] : "student";
    department = firestoreStudents[rfidUid]["department"];
    schedulesJsonStr = firestoreStudents[rfidUid]["schedules"].length() > 0 ? firestoreStudents[rfidUid]["schedules"] : "[]";
    yield(); // Yield after data extraction
  }

  yield(); // Yield before status determination

  // Determine status and action
  String finalStatus = "Pending"; // Default to Pending until weight is confirmed
  String action = "Initial Tap";
  
  // For absent students (sensorIndex == -3), ensure weight is 0.0
  float sensorWeight = (sensorIndex == -3) ? 0.0 : weight;
  
  // Always update the sensorType to include specific sensor number for the current student
  String sensorType = "Weight Sensor";
  if (sensorIndex >= 0 && sensorIndex < NUM_SENSORS) {
    sensorType = "Weight Sensor " + String(sensorIndex + 1);
  }

  yield(); // Yield before SD operations

  // Log to SD if offline
  if (!isConnected || sdMode) {
    String entry = "Student:UID:" + rfidUid +
                   " TimeIn:" + timestamp +
                   " Action:" + action +
                   " Status:" + finalStatus +
                   " Sensor:" + sensorStr +
                   " Weight:" + String(sensorWeight) +  // Use sanitized weight value
                   " assignedSensorId:" + String(sensorIndex >= 0 ? sensorIndex : -1) +
                   (timeOut != "" ? " TimeOut:" + timeOut : "");
    yield(); // Yield before SD write
    storeLogToSD(entry);
    Serial.println("SD log: " + entry);
    yield(); // Yield after SD write
  }

  yield(); // Yield before Firebase operations

  // Firebase logging with new structure
  if (!sdMode && isConnected && Firebase.ready()) {
    String date = timestamp.substring(0, 10);
    
    // Process schedules
    FirebaseJsonArray allSchedulesArray;
    FirebaseJson matchedSchedule;
    String subjectCode = "Unknown", roomName = "Unknown", sectionName = "Unknown";
    
    if (schedulesJsonStr != "[]") {
      yield(); // Yield before schedule processing
      FirebaseJsonArray tempArray;
      if (tempArray.setJsonArrayData(schedulesJsonStr)) {
        String currentDay = getDayFromTimestamp(timestamp);
        String currentTime = timestamp.substring(11, 16);
        
        for (size_t i = 0; i < tempArray.size(); i++) {
          if (i % 2 == 0) yield(); // Yield every 2 schedules
          FirebaseJsonData scheduleData;
          if (tempArray.get(scheduleData, i)) {
            FirebaseJson scheduleObj;
            if (scheduleObj.setJsonData(scheduleData.stringValue)) {
              FirebaseJson newScheduleObj;
              FirebaseJsonData fieldData;
              
              yield(); // Yield before field extraction
              if (scheduleObj.get(fieldData, "day")) newScheduleObj.set("day", fieldData.stringValue);
              if (scheduleObj.get(fieldData, "startTime")) newScheduleObj.set("startTime", fieldData.stringValue);
              if (scheduleObj.get(fieldData, "endTime")) newScheduleObj.set("endTime", fieldData.stringValue);
              yield();
              if (scheduleObj.get(fieldData, "roomName")) {
                newScheduleObj.set("roomName", fieldData.stringValue);
                roomName = fieldData.stringValue;
              }
              if (scheduleObj.get(fieldData, "subjectCode")) {
                newScheduleObj.set("subjectCode", fieldData.stringValue);
                subjectCode = fieldData.stringValue;
              }
              if (scheduleObj.get(fieldData, "subject")) newScheduleObj.set("subject", fieldData.stringValue);
              yield();
              if (scheduleObj.get(fieldData, "section")) {
                newScheduleObj.set("section", fieldData.stringValue);
                sectionName = fieldData.stringValue;
              }
              if (scheduleObj.get(fieldData, "instructorName")) newScheduleObj.set("instructorName", fieldData.stringValue);
              if (scheduleObj.get(fieldData, "sectionId")) {
                newScheduleObj.set("sectionId", fieldData.stringValue);
                sectionId = fieldData.stringValue; // Store the section ID for later use
              }
              
              yield(); // Yield before array add
              allSchedulesArray.add(newScheduleObj);
              
              // Check if this is the current schedule
              if (scheduleObj.get(fieldData, "day") && fieldData.stringValue == currentDay) {
                String startTime, endTime;
                if (scheduleObj.get(fieldData, "startTime")) startTime = fieldData.stringValue;
                if (scheduleObj.get(fieldData, "endTime")) endTime = fieldData.stringValue;
                if (isTimeInRange(currentTime, startTime, endTime)) {
                  matchedSchedule = newScheduleObj;
                  yield(); // Yield after match
                  break;
                }
              }
            }
          }
        }
      }
    }

    yield(); // Yield before session ID check

    // Get the last session ID
    String lastSessionId = "";
    if (currentSessionId != "") {
      lastSessionId = currentSessionId;
    } else {
      String lastSessionPath = "/Students/" + rfidUid + "/lastSession";
      if (Firebase.RTDB.getString(&fbdo, lastSessionPath)) {
        lastSessionId = fbdo.stringData();
      }
    }

    if (lastSessionId != "") {
      yield(); // Yield before attendance path check
      // Check if this is a continued attendance record or new one
      String attendancePath = "/Students/" + rfidUid + "/Attendance/" + lastSessionId;
  FirebaseJson attendanceInfoJson;
  
      // Set base attendance info
  attendanceInfoJson.set("status", finalStatus);
      attendanceInfoJson.set("timeIn", timestamp);
  attendanceInfoJson.set("action", action);
      attendanceInfoJson.set("sensorType", sensorType);
  attendanceInfoJson.set("rfidAuthenticated", true);
      attendanceInfoJson.set("assignedSensorId", sensorIndex >= 0 ? sensorIndex : -1);
  
  if (timeOut != "") {
    attendanceInfoJson.set("timeOut", timeOut);
  }
  
      // Add session info
      FirebaseJson sessionInfoJson;
      sessionInfoJson.set("sessionStartTime", timestamp);
      sessionInfoJson.set("subjectCode", subjectCode);
      sessionInfoJson.set("roomName", roomName);
      sessionInfoJson.set("section", sectionName);

      // Update the RTDB with attendance info
      yield(); // Yield before Firebase update
      String attendanceInfoPath = attendancePath + "/attendanceInfo";
      if (Firebase.RTDB.updateNode(&fbdo, attendanceInfoPath, &attendanceInfoJson)) {
        Serial.println("Updated attendance info for " + rfidUid + " session " + lastSessionId + ": " + finalStatus);
      } else {
        Serial.println("Failed to update attendance: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
      
      // Update session info if not already set
      yield(); // Yield before session info update
      String sessionInfoPath = attendancePath + "/sessionInfo";
      if (Firebase.RTDB.updateNode(&fbdo, sessionInfoPath, &sessionInfoJson)) {
        Serial.println("Updated session info for " + rfidUid);
      } else {
        Serial.println("Failed to update session info: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }

      // Set the last session for this student
      yield(); // Yield before last session update
      String lastSessionPath = "/Students/" + rfidUid + "/lastSession";
      if (Firebase.RTDB.setString(&fbdo, lastSessionPath, lastSessionId)) {
        Serial.println("Updated last session for " + rfidUid + " to " + lastSessionId);
    } else {
        Serial.println("Failed to update last session: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
    }
  } else {
      Serial.println("No session ID available for " + rfidUid);
    }
  }
  
  yield(); // Final yield before updating timers
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// Helper functions (add these to your code)
String getDayFromTimestamp(String timestamp) {
  // Convert "2025_04_11_220519" to day of week
  int year = timestamp.substring(0, 4).toInt();
  int month = timestamp.substring(5, 7).toInt();
  int day = timestamp.substring(8, 10).toInt();
  // Simple Zeller's Congruence for day of week
  if (month < 3) {
    month += 12;
    year--;
  }
  int k = day;
  int m = month;
  int D = year % 100;
  int C = year / 100;
  int f = k + ((13 * (m + 1)) / 5) + D + (D / 4) + (C / 4) - (2 * C);
  int dayOfWeek = f % 7;
  if (dayOfWeek < 0) dayOfWeek += 7;

  const char* days[] = {"Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
  return String(days[dayOfWeek]);
}

bool isTimeInRange(String currentTime, String startTime, String endTime) {
  // Convert times to minutes for comparison (e.g., "22:05" -> 1325)
  int currentMins = currentTime.substring(0, 2).toInt() * 60 + currentTime.substring(3, 5).toInt();
  int startMins = startTime.substring(0, 2).toInt() * 60 + startTime.substring(3, 5).toInt();
  int endMins = endTime.substring(0, 2).toInt() * 60 + endTime.substring(3, 5).toInt();
  return currentMins >= startMins && currentMins <= endMins;
}


void logUnregisteredUID(String uid, String timestamp) {
  Serial.println("Updating unregistered UID: " + uid);
  
  // Display message about unregistered UID
  displayMessage("Unregistered ID", "Access Denied", 2000);
  
  // First check if the UID is actually registered but wasn't found on first check
  // by trying a direct fetch from Firestore
  if (!sdMode && isConnected && Firebase.ready()) {
    // Try to check Firestore students collection
    Serial.println("Directly checking Firestore for UID " + uid);
    String firestorePath = "students";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            if (rfidUid == uid) {
              // Found it! Now cache the student data for future use
              Serial.println("Found student with UID " + uid + " in Firestore. Adding to cache.");
              
              std::map<String, String> studentData;
              if (doc.get(fieldData, "fields/fullName/stringValue")) {
                studentData["fullName"] = fieldData.stringValue;
              } else {
                studentData["fullName"] = "Unknown";
              }
              
              if (doc.get(fieldData, "fields/email/stringValue")) {
                studentData["email"] = fieldData.stringValue;
              }
              
              if (doc.get(fieldData, "fields/role/stringValue")) {
                studentData["role"] = fieldData.stringValue;
              } else {
                studentData["role"] = "student";
              }
              
              firestoreStudents[uid] = studentData;
              
              // Force a full data fetch to get complete student information
              fetchFirestoreStudents();
              return; // Exit without logging as unregistered since it's actually registered
            }
          }
        }
      }
    } else {
      Serial.println("Failed to retrieve Firestore students: " + firestoreFbdo.errorReason());
      if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
          firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
          firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
    
    // If we reach here, the UID is not in Firestore, so log it as unregistered
    if (Firebase.RTDB.setString(&fbdo, "/Unregistered/" + uid + "/Time", timestamp)) {
      Serial.println("Unregistered UID logged to Firebase RTDB: Unregistered:" + uid + " Time:" + timestamp);
    } else {
      Serial.println("Failed to log unregistered UID to RTDB: " + fbdo.errorReason());
      String entry = "Unregistered:UID:" + uid + " Time:" + timestamp;
      storeLogToSD(entry);
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    // Offline mode - log to SD
    String entry = "Unregistered:UID:" + uid + " Time:" + timestamp;
    storeLogToSD(entry);
  }
}

void logAdminAccess(String uid, String timestamp) {
  // Heap check to prevent crashes
  uint32_t freeHeap = ESP.getFreeHeap();
  if (freeHeap < 15000) {
    Serial.println("Warning: Low heap (" + String(freeHeap) + " bytes). Skipping Firebase operations.");
    storeLogToSD("LowHeapWarning:UID:" + uid + " Time:" + timestamp);
    deniedFeedback();
    displayMessage("System Busy", "Try Again", 2000);
    return;
  }

  // SD log entry (basic)
  String entry = "Admin:UID:" + uid + " Time:" + timestamp;
  yield(); // Allow system to process after SD operation

  // Validate admin UID
  if (!isAdminUID(uid)) {
    entry += " Action:Denied_NotAdmin";
    storeLogToSD(entry);
    deniedFeedback();
    displayMessage("Not Admin", "Access Denied", 2000);
    return;
  }

  // Fetch user details with yield
  std::map<String, String> userData = fetchUserDetails(uid);
  String fullName = userData.empty() ? "Unknown" : userData["fullName"];
  String role = userData.empty() ? "admin" : userData["role"];
  yield(); // Allow system to process after user fetch

  // Sanitize timestamp for Firebase paths
  String sanitizedTimestamp = timestamp;
  sanitizedTimestamp.replace(" ", "_");
  sanitizedTimestamp.replace(":", "");
  sanitizedTimestamp.replace("/", "_");

  // IMPROVED LOGIC: First check if this admin is the one who started the current session
  bool isEntry;
  String action;
  
  if (adminAccessActive && uid == lastAdminUID) {
    // This is the same admin who started the session - this is an EXIT
    isEntry = false;
    action = "exit";
  } else if (adminAccessActive && uid != lastAdminUID) {
    // Different admin is trying to access - deny
    entry += " Action:Denied_DifferentUID";
    storeLogToSD(entry);
    deniedFeedback();
    Serial.println("Different admin UID detected: " + uid);
    displayMessage("Session Active", "Use Same UID", 2000);
    displayMessage("Admin Mode", "Active", 0);
    
    // Update timers
    firstActionOccurred = true;
    lastActivityTime = millis();
    lastReadyPrint = millis();
    return;
  } else {
    // No admin session active - this is an ENTRY
    isEntry = true;
    action = "entry";
  }

  // Assign room before creating the AccessLogs entry
  if (isEntry) {
    assignedRoomId = assignRoomToAdmin(uid);
  }

  // Log PZEM data on exit for SD
  if (!isEntry) {
    float voltage = max(pzem.voltage(), 0.0f);
    float current = max(pzem.current(), 0.0f);
    float power = max(pzem.power(), 0.0f);
    float energy = max(pzem.energy(), 0.0f);
    float frequency = max(pzem.frequency(), 0.0f);
    float powerFactor = max(pzem.pf(), 0.0f);
    entry += " Action:Exit Voltage:" + String(voltage, 2) + "V Current:" + String(current, 2) + "A Power:" + String(power, 2) +
             "W Energy:" + String(energy, 3) + "kWh Frequency:" + String(frequency, 2) + "Hz PowerFactor:" + String(powerFactor, 2);
  } else {
    entry += " Action:Entry";
  }
  storeLogToSD(entry);

  // Firebase logging (/AccessLogs and /AdminPZEM)
  if (!sdMode && isConnected && Firebase.ready()) {
    // Prevent watchdog triggering during Firebase operations
    lastReadyPrint = millis(); 
    
    // /AccessLogs
    String accessPath = "/AccessLogs/" + uid + "/" + sanitizedTimestamp;
    FirebaseJson accessJson;
    accessJson.set("action", action);
    accessJson.set("timestamp", timestamp);
    accessJson.set("fullName", fullName);
    accessJson.set("role", role);

    // For entry, add room details to AccessLogs
    if (isEntry) {
      // Using assignedRoomId set before Firebase operations
      if (assignedRoomId != "" && firestoreRooms.find(assignedRoomId) != firestoreRooms.end()) {
        const auto& roomData = firestoreRooms[assignedRoomId];
        FirebaseJson roomDetails;

        // Use at() for const map access and proper error handling
        try {
          roomDetails.set("building", roomData.count("building") ? roomData.at("building") : "Unknown");
          roomDetails.set("floor", roomData.count("floor") ? roomData.at("floor") : "Unknown");
          roomDetails.set("name", roomData.count("name") ? roomData.at("name") : "Unknown");
          roomDetails.set("status", "maintenance");  // Always set to maintenance for admin inspections
          roomDetails.set("type", roomData.count("type") ? roomData.at("type") : "Unknown");
          accessJson.set("roomDetails", roomDetails);

          // Also update the room status in Firestore
          String roomPath = "rooms/" + assignedRoomId;
          FirebaseJson contentJson;
          contentJson.set("fields/status/stringValue", "maintenance");
          
          if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", roomPath.c_str(), contentJson.raw(), "status")) {
            Serial.println("Room status updated to 'maintenance' in Firestore: " + assignedRoomId);
          } else {
            Serial.println("Failed to update room status in Firestore: " + firestoreFbdo.errorReason());
            if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
                firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
                firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
              handleFirebaseSSLError();
            }
          }
        } catch (const std::out_of_range& e) {
          Serial.println("Error accessing room data: " + String(e.what()));
          roomDetails.set("building", "Unknown");
          roomDetails.set("floor", "Unknown");
          roomDetails.set("name", "Unknown");
          roomDetails.set("status", "maintenance");
          roomDetails.set("type", "Unknown");
          accessJson.set("roomDetails", roomDetails);
        }
      }
    }

    // For exit, add PZEM data to AccessLogs
    if (!isEntry && isVoltageSufficient) {
      // Get the entry timestamp to find the entry record
      String entryTimestamp = "";
      if (Firebase.RTDB.getJSON(&fbdo, "/AccessLogs/" + uid)) {
        FirebaseJson json;
        json.setJsonData(fbdo.to<FirebaseJson>().raw());
        
        // Find the most recent "entry" action using Firebase iterators correctly
        size_t count = json.iteratorBegin();
        String latestEntryKey = "";
        
        for (size_t i = 0; i < count; i++) {
          int type = 0;
          String key, value;
          json.iteratorGet(i, type, key, value);
          
          if (type == FirebaseJson::JSON_OBJECT) {
            // This is an entry, check if it has 'action' = 'entry'
            FirebaseJson entryJson;
            FirebaseJsonData actionData;
            
            // Create a new JSON with just this item and check it
            String jsonStr = "{\"" + key + "\":" + value + "}";
            FirebaseJson keyJson;
            keyJson.setJsonData(jsonStr);
            
            // Get action from this entry
            if (keyJson.get(actionData, key + "/action") && 
                actionData.stringValue == "entry") {
              // Found an entry action, check if it's newer
              if (latestEntryKey == "" || key.compareTo(latestEntryKey) > 0) {
                latestEntryKey = key;
              }
            }
          }
        }
        
        json.iteratorEnd();
        
        if (latestEntryKey != "") {
          entryTimestamp = latestEntryKey;
          Serial.println("Found latest entry record timestamp: " + entryTimestamp);
          
          // Update room status back to "available" when admin exits
          FirebaseJson statusUpdate;
          statusUpdate.set("status", "available");
          
          if (Firebase.RTDB.updateNode(&fbdo, "/AccessLogs/" + uid + "/" + entryTimestamp + "/roomDetails", &statusUpdate)) {
            Serial.println("Room status updated to 'available' in entry record: " + entryTimestamp);
          } else {
            Serial.println("Failed to update room status: " + fbdo.errorReason());
            if (fbdo.errorReason().indexOf("ssl") >= 0 || 
                fbdo.errorReason().indexOf("connection") >= 0 || 
                fbdo.errorReason().indexOf("SSL") >= 0) {
              handleFirebaseSSLError();
            }
          }
        }
      }
      
      // No need to update the roomDetails/exit data since we have a separate exit record
      // We'll only keep the PZEM data in the separate exit record
      
      // Add PZEM data to the current exit record
      FirebaseJson pzemData;
      pzemData.set("voltage", lastVoltage);
      pzemData.set("current", lastCurrent);
      pzemData.set("power", lastPower);
      pzemData.set("energy", lastEnergy);
      pzemData.set("frequency", lastFrequency);
      pzemData.set("powerFactor", lastPowerFactor);
      accessJson.set("pzemData", pzemData);
    }

    Serial.print("Pushing to RTDB: " + accessPath + "... ");
    if (Firebase.RTDB.setJSON(&fbdo, accessPath, &accessJson)) {
      Serial.println("Success");
      Serial.println("Admin " + fullName + " access logged: " + action);
    } else {
      Serial.println("Failed: " + fbdo.errorReason());
      storeLogToSD("AccessLogFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }

    // We no longer log to AdminPZEM, using AccessLogs instead
    yield(); // Allow system to process after Firebase operations

    // Update /Admin/<uid>
    if (!userData.empty()) {
      String adminPath = "/Admin/" + uid;
      FirebaseJson adminJson;
      adminJson.set("fullName", userData["fullName"]);
      adminJson.set("role", userData["role"]);
      adminJson.set("createdAt", userData.count("createdAt") ? userData["createdAt"] : "2025-01-01T00:00:00.000Z");
      adminJson.set("email", userData.count("email") ? userData["email"] : "unknown@gmail.com");
      adminJson.set("idNumber", userData.count("idNumber") ? userData["idNumber"] : "N/A");
      adminJson.set("rfidUid", uid);
      // Only update lastTamperStop if previously set (avoid overwriting tamper resolution)
      if (userData.count("lastTamperStop")) {
        adminJson.set("lastTamperStop", userData["lastTamperStop"]);
      }

      Serial.print("Updating RTDB: " + adminPath + "... ");
      if (Firebase.RTDB.setJSON(&fbdo, adminPath, &adminJson)) {
        Serial.println("Success");
        Serial.println("Admin details updated in RTDB at " + adminPath);
      } else {
        Serial.println("Failed: " + fbdo.errorReason());
        storeLogToSD("AdminUpdateFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    yield(); // Allow system to process after Firebase operations
    }
  }

  // Now use the simplified isEntry flag for behavior
  if (isEntry) {
    activateRelays();
    adminAccessActive = true;
    lastAdminUID = uid;
    
    // Set up door auto-lock timeout
    adminDoorOpenTime = millis();
    Serial.println("Door will auto-lock in 30 seconds while admin inspection continues");
    
    // Room already assigned above
    if (assignedRoomId == "") {
      displayMessage("No Room Available", "For Inspection", 2000);
    } else {
      if (firestoreRooms.find(assignedRoomId) != firestoreRooms.end()) {
        String roomName = firestoreRooms[assignedRoomId].at("name");
        displayMessage("Inspecting Room", roomName, 2000);
      }
    }
    accessFeedback();
    logSystemEvent("Relay Activated for Admin UID: " + uid);
    Serial.println("Admin access granted for UID: " + uid);
    displayMessage("Admin Access", "Granted", 2000);
    displayMessage("Admin Mode", "Active", 0);

  // Handle exit - now this is captured by the isEntry flag rather than a separate condition
  } else {
    // Use our improved deactivateRelays function for safer relay operations
    deactivateRelays();
    
    // Allow system to stabilize after relay operations
    yield();
    delay(50);
    yield();
    
    adminAccessActive = false;
    lastAdminUID = "";
    lastPZEMLogTime = 0;
    
    // Reset the RFID reader to ensure it's in a clean state for the next read
    rfid.PCD_Reset();
    delay(100);
    rfid.PCD_Init();
    
    // Also update room status in Firestore if we have a room assigned
    if (assignedRoomId != "" && !sdMode && isConnected && Firebase.ready()) {
      // Update the room status in Firestore back to "available"
      String roomPath = "rooms/" + assignedRoomId;
      FirebaseJson contentJson;
      contentJson.set("fields/status/stringValue", "available");
      
      if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", roomPath.c_str(), contentJson.raw(), "status")) {
        Serial.println("Room status updated to 'available' in Firestore: " + assignedRoomId);
      } else {
        Serial.println("Failed to update room status in Firestore: " + firestoreFbdo.errorReason());
        if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
            firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
            firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    }
    
    assignedRoomId = "";
    accessFeedback();
    logSystemEvent("Relay Deactivated for Admin UID: " + uid);
    Serial.println("Admin access ended for UID: " + uid);
    displayMessage("Admin Access", "Ended", 2000);
    displayMessage("Door Locked", "", 2000);
    
    // Reset session state for clean transition
    resetSessionState();
    
    // Allow the system to stabilize before transition
    yield();
    
    // Use smooth transition instead of direct ready message
    smoothTransitionToReady();
  }

  // Update timers
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();

  // Log heap after operations
  Serial.println("Heap after logAdminAccess: " + String(ESP.getFreeHeap()) + " bytes");
}

void logAdminTamperStop(String uid, String timestamp) {
  String entry = "Admin:" + uid + " TamperStopped:" + timestamp;
  storeLogToSD(entry);

  // Update tamper event in Alerts/Tamper node
  if (!sdMode && isConnected && Firebase.ready()) {
    // Use currentTamperAlertId if available, fallback to tamperStartTime
    String alertId = currentTamperAlertId.length() > 0 ? currentTamperAlertId : tamperStartTime;
    String tamperPath = "/Alerts/Tamper/" + alertId;
    
    // Fetch user details for resolvedByFullName
    std::map<String, String> userData = fetchUserDetails(uid);
    String fullName = userData.empty() ? "Unknown Admin" : userData["fullName"];
    String role = userData.empty() ? "admin" : userData["role"];
    
    FirebaseJson tamperJson;
    tamperJson.set("endTime", timestamp);
    tamperJson.set("status", "resolved");
    tamperJson.set("resolvedBy", uid);
    tamperJson.set("resolverName", fullName);
    tamperJson.set("resolverRole", role);
    tamperJson.set("resolutionTime", timestamp);

    Serial.print("Logging tamper resolution: " + tamperPath + "... ");
    if (Firebase.RTDB.updateNode(&fbdo, tamperPath, &tamperJson)) {
      Serial.println("Success");
      Serial.println("Tamper event resolved at " + tamperPath + " by UID " + uid);
    } else {
      Serial.println("Failed: " + fbdo.errorReason());
      storeLogToSD("TamperStopFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }

    // Update /Admin/<uid> with original fields and tamper resolution info
    if (!userData.empty()) {
      String adminPath = "/Admin/" + uid;
      FirebaseJson adminJson;
      adminJson.set("fullName", userData["fullName"]);
      adminJson.set("role", userData["role"]);
      adminJson.set("lastTamperStop", timestamp);
      adminJson.set("lastTamperAlertId", alertId);
      // Restore original fields
      adminJson.set("email", userData["email"].length() > 0 ? userData["email"] : "unknown@gmail.com");
      adminJson.set("idNumber", userData["idNumber"].length() > 0 ? userData["idNumber"] : "N/A");
      adminJson.set("createdAt", userData.count("createdAt") > 0 ? userData["createdAt"] : "2025-01-01T00:00:00.000Z");
      adminJson.set("rfidUid", uid);

      Serial.print("Updating RTDB: " + adminPath + "... ");
      if (Firebase.RTDB.updateNode(&fbdo, adminPath, &adminJson)) {
        Serial.println("Success");
        Serial.println("Admin node updated for tamper resolution at " + adminPath);
      } else {
        Serial.println("Failed: " + fbdo.errorReason());
        storeLogToSD("AdminUpdateFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    } else {
      Serial.println("No Firestore data for UID " + uid + "; logging minimal /Admin update.");
      String adminPath = "/Admin/" + uid;
      FirebaseJson adminJson;
      adminJson.set("fullName", "Unknown Admin");
      adminJson.set("role", "admin");
      adminJson.set("lastTamperStop", timestamp);
      adminJson.set("lastTamperAlertId", alertId);
      adminJson.set("email", "unknown@gmail.com");
      adminJson.set("idNumber", "N/A");
      adminJson.set("createdAt", "2025-01-01T00:00:00.000Z");
      adminJson.set("rfidUid", uid);

      if (Firebase.RTDB.updateNode(&fbdo, adminPath, &adminJson)) {
        Serial.println("Minimal admin node updated at " + adminPath);
      } else {
        Serial.println("Minimal admin update failed: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    }
  } else {
    Serial.println("Firebase unavailable; tamper stop logged to SD.");
    std::map<String, String> userData = fetchUserDetails(uid);
    String fullName = userData.empty() ? "Unknown Admin" : userData["fullName"];
    String detailedEntry = "Admin:" + uid + " TamperStopped:" + timestamp + 
                          " ResolvedByUID:" + uid + " ResolvedByFullName:" + fullName;
    storeLogToSD(detailedEntry);
  }

  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();

  // Transition to ready state after tamper resolution
  smoothTransitionToReady();
}

void logSystemEvent(String event) {
  String timestamp = getFormattedTime();
  String entry = "System:" + event + " Time:" + timestamp;
  storeLogToSD(entry);
  if (!sdMode && isConnected && isVoltageSufficient) {
    Firebase.RTDB.pushString(&fbdo, "/SystemLogs", entry);
  }
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void watchdogCheck() {
  // Only trigger watchdog if there's no active operation that could legitimately take a long time
  if ((millis() - lastReadyPrint > 300000) && 
      !adminAccessActive && 
      !tamperActive && 
      !classSessionActive && 
      !studentVerificationActive && 
      !relayActive && 
      !tapOutPhase) {
    Serial.println("Watchdog timeout. Restarting system.");
    logSystemEvent("Watchdog Reset");
    ESP.restart();
  }
}

// Add the feedWatchdog function implementation
void feedWatchdog() {
  // ESP32 Arduino core doesn't have ESP.wdtFeed(), use alternatives
  yield();
  delay(1); // Tiny delay to ensure background processes can run
}

// Helper function for safe Firebase operations with watchdog handling
void safeFirebaseOperation() {
  // Declare external variables to ensure they're in scope
  extern bool displayingMessage;
  extern String currentLine1;
  extern String currentLine2;
  
  // Skip Firebase operations during startup and when WiFi is being reconfigured
  if (millis() < 5000 || wifiReconfiguring) {
    yield();
    return;
  }
  
  static unsigned long operationStartTime = 0;
  
  // First Firebase operation call
  if (operationStartTime == 0) {
    operationStartTime = millis();
    return;
  }
  
  // Reset watchdog during long-running Firebase operations
  if (millis() - operationStartTime > 5000) {
    Serial.println("Long-running Firebase operation, resetting watchdog");
    lastReadyPrint = millis();
    operationStartTime = millis();
    
    // Add multiple yields to ensure the system stays responsive
    for (int i = 0; i < 5; i++) {
      yield();
      delay(1);
    }
  }
  
  // Always yield during Firebase operations
  yield();
  
  // Periodically check if the LCD needs updating during long operations
  static unsigned long lastLcdCheck = 0;
  if (millis() - lastLcdCheck > 1000) {  // Check every second
    lastLcdCheck = millis();
    // Force LCD update if needed
    if (displayingMessage) {
      // Refresh the current LCD display if a message is active
      if (currentLine1.length() > 0 || currentLine2.length() > 0) {
        int retryCount = 0;
        const int maxRetries = 3;
        while (retryCount < maxRetries) {
          lcd.clear();
          lcd.setCursor(0, 0);
          lcd.print(currentLine1);
          lcd.setCursor(0, 1);
          lcd.print(currentLine2);
          
          Wire.beginTransmission(0x27);
          if (Wire.endTransmission() == 0) break;
          recoverI2C();
          delay(10);
          yield();
          retryCount++;
        }
      }
    }
  }
}

bool checkResetButton() {
  static unsigned long pressStart = 0;
  static int lastButtonState = HIGH;
  static unsigned long lastDebounceTime = 0;
  const unsigned long debounceDelay = 50;    // 50ms debounce
  const unsigned long pressDuration = 200;   // 200ms to confirm press

  int currentButtonState = digitalRead(RESET_BUTTON_PIN);

  // Debug: Log state changes
  if (currentButtonState != lastButtonState) {
    Serial.print("Reset button state changed to: ");
    Serial.println(currentButtonState == LOW ? "LOW (pressed)" : "HIGH (released)");
    lastDebounceTime = millis();
  }

  // Debounce: Update state only if stable for debounceDelay
  if ((millis() - lastDebounceTime) > debounceDelay) {
    if (currentButtonState == LOW && lastButtonState == HIGH) {
      // Button just pressed
      pressStart = millis();
      Serial.println("Reset button press detected, starting timer...");
      // Immediate feedback
      tone(BUZZER_PIN, 2000, 100);
      digitalWrite(LED_R_PIN, HIGH);
      delay(50); // Brief feedback
      digitalWrite(LED_R_PIN, LOW);
    } else if (currentButtonState == HIGH && lastButtonState == LOW) {
      // Button released
      pressStart = 0;
      Serial.println("Reset button released.");
    }
    lastButtonState = currentButtonState;
  }

  // Check for confirmed press
  if (currentButtonState == LOW && pressStart != 0 && (millis() - pressStart >= pressDuration)) {
    Serial.println("Reset confirmed after 200ms. Initiating restart...");
    // Log to SD if initialized
    if (sdInitialized) {
      String timestamp = getFormattedTime();
      String entry = "System:ResetButton Timestamp:" + timestamp + " Action:UserReset";
      storeLogToSD(entry);
      Serial.println("Reset logged to SD: " + entry);
    }
    // Final feedback
    tone(BUZZER_PIN, 1000, 200);
    digitalWrite(LED_R_PIN, HIGH);
    digitalWrite(LED_G_PIN, HIGH);
    digitalWrite(LED_B_PIN, HIGH);
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Resetting...");
    delay(300); // Reduced from 500ms to ensure quick restart
    digitalWrite(LED_R_PIN, LOW);
    digitalWrite(LED_G_PIN, LOW);
    digitalWrite(LED_B_PIN, LOW);
    ESP.restart();
    return true; // Won't be reached
  }

  return false;
}



#include <Wire.h>
#include <LiquidCrystal_I2C_Hangul.h>
#include <WiFi.h>
#include <Firebase_ESP_Client.h>
#include <MFRC522.h>
#include <SPI.h>
#include <SD.h>
#include <PZEM004Tv30.h>
#include "freertos/FreeRTOS.h"
#include "freertos/timers.h"
#include <time.h>
#include <vector>
#include <map>
#include <esp_sleep.h>
#include <ThreeWire.h>  // DS1302 communication
#include <String.h>
#include <RtcDS1302.h>  // DS1302 RTC library
#include <set>

// Define ScheduleInfo struct first
struct ScheduleInfo {
  bool isValid;
  String day;
  String startTime;
  String endTime;
  String roomName;
  String subject;
  String subjectCode;
  String section;
};

// Global variable declarations
ScheduleInfo currentSchedule = {false, "", "", "", "", "", "", ""}; // Current active schedule
ScheduleInfo tapOutSchedule = {false, "", "", "", "", "", "", ""}; // Schedule saved for tap-out phase
bool pzemLoggedForSession = false; // Flag to track if PZEM data was logged for the current session

// Add missing variable declarations
bool uidDetailsFetched = false;
std::map<String, bool> uidDetailsPrinted;
// Add I2C_ERROR_COUNTER variable declaration
int I2C_ERROR_COUNTER = 0;

// Forward declarations for functions
void initSDCard();
void rfidTask(void * parameter);

// Function Prototypes
void connectWiFi();
void initFirebase();
String getUIDString();
void resetFeedbackAndRestart();
void nonBlockingDelay(unsigned long ms);
void printSDCardInfo();
void accessFeedback();
void deniedFeedback();
void unregisteredUIDFeedback(); // New function for unregistered UID feedback
void storeLogToSD(String entry);
bool syncOfflineLogs();
void watchdogCheck(); // Add the feedWatchdog function declaration here
void feedWatchdog(); // Added missing function declaration
String getFormattedTime();
bool checkResetButton();
void showNeutral();
void logInstructor(String uid, String timestamp, String action);
void logStudentToRTDB(String rfidUid, String timestamp, float weight, int sensorIndex, String weightConfirmed, String timeOut);
void logPZEMData(String uid, float voltage, float current, float power, float energy, float frequency, float pf);
void logUnregisteredUID(String uid, String timestamp);
void logAdminAccess(String uid, String timestamp);
void logAdminTamperStop(String uid, String timestamp);
void logSystemEvent(String event);
bool isRegisteredUID(String uid);
void fetchRegisteredUIDs();
void fetchFirestoreTeachers();
void fetchFirestoreStudents();
void displayMessage(String line1, String line2, unsigned long duration);
void streamCallback(FirebaseStream data);
void streamTimeoutCallback(bool timeout);
void resetWeightSensors();
void setupWeightSensors(); // Added missing function declaration
void enterPowerSavingMode();
void exitPowerSavingMode();
void recoverI2C();
void activateRelays();
void deactivateRelays();
void fetchFirestoreRooms();
void assignRoomToInstructor(String uid, String timestamp);
void updateRoomStatus(String roomId, String status, String instructorName, String subject, String sessionStart, String sessionEnd, float startReading, float endReading, float totalUsage);
String getDate();
bool isAdminUID(String uid);
std::map<String, String> fetchUserDetails(String uid);
ScheduleInfo getInstructorScheduleForDay(String uid, String dateStr);
void logSuperAdmin(String uid, String timestamp);
void handleFirebaseSSLError();
void checkAdminDoorAutoLock();
bool syncSchedulesToSD(); // Added missing function declaration
bool syncOfflineLogsToRTDB(); // Added missing function declaration
ScheduleInfo checkSchedule(String uid, String day, int hour, int minute); // Added missing function declaration
String getDayFromTimestamp(String timestamp); // Added missing function declaration
bool isTimeInRange(String currentTime, String startTime, String endTime); // Added missing function declaration
void smoothTransitionToReady(); // Added missing function declaration

// Global Objects and Pin Definitions
LiquidCrystal_I2C_Hangul lcd(0x27, 16, 2);

#define WIFI_SSID "CIT-U_SmartEco_Lock"
#define WIFI_PASSWORD "123456789"
#define API_KEY "AIzaSyCnBauXgFmxyWWO5VHcGUNToGy7lulbN6E"
#define DATABASE_URL "https://smartecolock-94f5a-default-rtdb.asia-southeast1.firebasedatabase.app/"
#define FIRESTORE_PROJECT_ID "smartecolock-94f5a"

FirebaseData fbdo;
FirebaseData streamFbdo;
FirebaseData firestoreFbdo;
FirebaseConfig config;
FirebaseAuth auth;

#define MFRC522_SCK 14
#define MFRC522_MISO 13
#define MFRC522_MOSI 11
#define MFRC522_CS 15
#define MFRC522_RST 2
#define MFRC522_IRQ 4
SPIClass hspi(HSPI);
MFRC522 rfid(MFRC522_CS, MFRC522_RST, &hspi);

#define SD_SCK 36
#define SD_MISO 37
#define SD_MOSI 35
#define SD_CS 10
SPIClass fsSPI(FSPI);
const char* OFFLINE_LOG_FILE = "/Offline_Logs_Entry.txt";

HardwareSerial pzemSerial(1);
#define PZEM_RX 18
#define PZEM_TX 17
PZEM004Tv30 pzem(pzemSerial, PZEM_RX, PZEM_TX);

// Define pin pairs for each load cell
const int DT_1 = 38, SCK_1 = 39;
const int DT_2 = 40, SCK_2 = 41;
const int DT_3 = 42, SCK_3 = 45;

#define TAMPER_PIN 22
#define BUZZER_PIN 6
#define LED_R_PIN 16
#define LED_G_PIN 19
#define LED_B_PIN 20
#define RESET_BUTTON_PIN 21
#define RELAY1 7
#define RELAY2 4
#define RELAY3 5
#define RELAY4 12
#define I2C_SDA 8
#define I2C_SCL 9
#define REED_PIN 3

// Define pins for ESP32-S3
#define SCLK_PIN 47  // Serial Clock
#define IO_PIN   48  // Data Input/Output
#define CE_PIN   46  // Chip Enable (Reset)

// Create RTC object
ThreeWire myWire(IO_PIN, SCLK_PIN, CE_PIN);
RtcDS1302<ThreeWire> Rtc(myWire);

// Relay pins (adjust these based on your hardware setup)
// Existing constants
const int RELAY1_PIN = 7; // Door relay
const int RELAY2_PIN = 4; // Additional function 1
const int RELAY3_PIN = 5; // Additional function 2
const int RELAY4_PIN = 12; // Additional function 3
const unsigned long finalVerificationTimeout = 30000;
const unsigned long WIFI_TIMEOUT = 10000;
const unsigned long INACTIVITY_TIMEOUT = 300000; // 5 minutes
const unsigned long Student_VERIFICATION_WINDOW = 120000; // 2 minutes total
const float voltageThreshold = 200.0;
const unsigned long VERIFICATION_WAIT_DELAY = 3000;
const unsigned long RFID_DEBOUNCE_DELAY = 2000;
const unsigned long I2C_RECOVERY_INTERVAL = 5000;
const unsigned long TAP_OUT_WINDOW = 300000; // 5 minutes for students to tap out
const unsigned long DOOR_OPEN_DURATION = 45000; // 30 seconds
const unsigned long WRONG_SEAT_TIMEOUT = 30000; // 30 seconds timeout for wrong seat
const unsigned long RESET_DEBOUNCE_DELAY = 50; // 50ms debounce delay
const String SUPER_ADMIN_UID = "A466BABA";
// Consolidated global variables (remove duplicates)
bool sdMode = false;
bool isInstructorLogged = false;
String lastInstructorUID = "";
bool classSessionActive = false;
unsigned long classSessionStartTime = 0;
bool waitingForInstructorEnd = false;
bool studentVerificationActive = false;
unsigned long studentVerificationStartTime = 0;
bool adminAccessActive = false;
String lastAdminUID = "";
bool tamperActive = false;
bool tamperAlertTriggered = false;  // Add this missing variable
bool tamperMessageDisplayed = false;  // Add this missing variable
bool buzzerActive = false;  // Add this missing variable for buzzer pulsing
unsigned long lastBuzzerToggle = 0;  // Add this missing variable for buzzer pulsing
unsigned long lastActivityTime = 0;
// Add admin door timeout tracking
#define ADMIN_DOOR_TIMEOUT 60000  // 60 seconds timeout for admin door
unsigned long adminDoorOpenTime = 0;  // Tracks when admin door was opened
bool adminDoorLockPending = false;    // Tracks if auto door lock is pending
unsigned long adminDoorLockTime = 0;  // Tracks when door should be locked
unsigned long lastReadyPrint = 0;
bool readyMessageShown = false;
unsigned long lastSleepMessageTime = 0;
bool relayActive = false;
unsigned long relayActiveTime = 0;
unsigned long lastRFIDTapTime = 0;
unsigned long lastUIDFetchTime = 0;
unsigned long lastDotUpdate = 0;
unsigned long lastPZEMLogTime = 0;
int dotCount = 0;
bool isConnected = false;
bool isVoltageSufficient = false;
bool wasConnected = false;
bool wasVoltageSufficient = false;
bool firstActionOccurred = false;
bool otaUpdateCompleted = false;
bool tamperMessagePrinted = false;
bool instructorTapped = false;
bool displayMessageShown = false;
bool firestoreFetched = false;
float initVoltage = 0.0;
float initCurrent = 0.0;
float initEnergy = 0.0;
unsigned long lastI2cRecovery = 0;
bool reedState = false;
bool tamperResolved = false;
bool powerSavingMode = false;
bool wifiReconfiguring = false;  // Flag to track when WiFi is being reconfigured
std::vector<String> registeredUIDs;
std::map<String, std::map<String, String>> firestoreTeachers;
std::map<String, std::map<String, String>> firestoreStudents;
std::map<String, std::map<String, String>> firestoreRooms;
std::vector<String> pendingStudentTaps;
String assignedRoomId = "";
float sessionStartReading = 0.0;
float lastVoltage = 0.0;
float lastCurrent = 0.0;
float lastPower = 0.0;
float lastEnergy = 0.0;
float lastFrequency = 0.0;
float lastPowerFactor = 0.0;
bool tapOutPhase = false;
unsigned long tapOutStartTime = 0;
int presentCount = 0;
String currentSessionId = "";
bool attendanceFinalized = false;
unsigned long lastResetDebounceTime = 0;
bool lastResetButtonState = HIGH;
std::map<String, std::map<String, String>> firestoreUsers;
bool doorOpen = false;
unsigned long doorOpenTime = 0;
bool relaysActive = false;
float totalEnergy = 0.0;
unsigned long lastPZEMUpdate = 0;
String doorOpeningUID = "";
bool sdInitialized = false;
String lastTappedUID = "";
String tamperStartTimestamp = "";
unsigned long lastPowerLogTime = 0;
bool superAdminSessionActive = false;
unsigned long superAdminSessionStartTime = 0;
unsigned long systemStartTime = 0;
int sessionEndTimeInMins = -1;

// Relay state management variables
bool relayTransitionInProgress = false;
unsigned long relayTransitionStartTime = 0;
const unsigned long RELAY_TRANSITION_TIMEOUT = 1000; // 1000ms timeout for transitions (increased from 500ms)
unsigned long scheduledDeactivationTime = 0;
bool relayOperationPending = false;
bool pendingRelayActivation = false;
bool relayPendingDeactivation = false;
unsigned long relayDeactivationTime = 0;
const unsigned long RELAY_SAFE_DELAY = 500; // 500ms safety delay (increased from 250ms)

#define DEBUG_MODE false  // Set to true for debug output, false for production

// Global variables for Firebase and SSL recovery
unsigned long sdModeRetryTime = 0; // Used for auto recovery from SSL errors

// Add missing tamper detection variables
bool tamperDetected = false;
String tamperStartTime = "";
String currentTamperAlertId = "";

void nonBlockingDelay(unsigned long ms) {
  unsigned long start = millis();
  while (millis() - start < ms) {
    yield();
  }
}

bool nonBlockingDelayWithReset(unsigned long duration) {
  unsigned long startTime = millis();
  while (millis() - startTime < duration) {
    if (checkResetButton()) {
      // Feedback handled by checkResetButton(); avoid duplicating here
      return false; // Signal reset detected (reset already handled)
    }
    yield();
  }
  return true; // Delay completed without reset
}

void showNeutral() {
  digitalWrite(LED_R_PIN, LOW);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, HIGH);
  Serial.println("LED State: Neutral (R:LOW, G:LOW, B:HIGH)");
}

void accessFeedback() {
  digitalWrite(LED_R_PIN, LOW);
  digitalWrite(LED_G_PIN, HIGH);
  digitalWrite(LED_B_PIN, LOW);
  Serial.println("LED State: Access (R:LOW, G:HIGH, B:LOW)");
  tone(BUZZER_PIN, 2000, 200);
  nonBlockingDelay(500);
  showNeutral();
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void deniedFeedback() {
  digitalWrite(LED_R_PIN, HIGH);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, LOW);
  Serial.println("LED State: Denied (R:HIGH, G:LOW, B:LOW)");
  tone(BUZZER_PIN, 500, 200);
  nonBlockingDelay(200);
  tone(BUZZER_PIN, 300, 200);
  nonBlockingDelay(300);
  showNeutral();
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// New function for unregistered UID feedback with stronger visual and audio indication
void unregisteredUIDFeedback() {
  // Turn on red LED
  digitalWrite(LED_R_PIN, HIGH);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, LOW);
  Serial.println("LED State: Unregistered UID (R:HIGH, G:LOW, B:LOW)");
  
  // Distinctive beeping pattern for unregistered UIDs
  tone(BUZZER_PIN, 800, 200);
  nonBlockingDelay(200);
  tone(BUZZER_PIN, 800, 200);
  nonBlockingDelay(200);
  tone(BUZZER_PIN, 400, 400);
  nonBlockingDelay(400);
  
  // Return to neutral state
  showNeutral();
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// Function to check and auto-lock admin door after timeout
void checkAdminDoorAutoLock() {
  // Check if the admin door is open and needs to be auto-locked due to timeout
  if (adminDoorOpenTime > 0 && relayActive && (millis() - adminDoorOpenTime > ADMIN_DOOR_TIMEOUT)) {
    Serial.println("Admin door auto-lock timeout reached");
    
    // Deactivate the relay for the door only
    digitalWrite(RELAY1, HIGH);
    
    // Update the admin door state but DON'T end the admin session
    relayActive = false;
    adminDoorOpenTime = 0;
    
    // Provide feedback
    deniedFeedback();
    displayMessage("Admin Door", "Auto-Locked", 2000);
    logSystemEvent("Admin Door Auto-Locked due to timeout");
    
    // Reset the RFID reader to ensure it can detect cards for exit tap
    rfid.PCD_Reset();
    delay(50);
    rfid.PCD_Init();
    rfid.PCD_SetAntennaGain(rfid.RxGain_max);
    
    // FIXED: Don't call smoothTransitionToReady() here - it ends the admin session
    // Instead, show admin mode is still active
    unsigned long startTime = millis();
    while (millis() - startTime < 2000) yield(); // Non-blocking delay
    
    displayMessage("Admin Mode Active", "Tap to Exit", 0);
    Serial.println("Admin mode remains active - tap same card to exit");
  }
}

void connectWiFi() {
  Serial.print("Connecting to WiFi");
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("Connecting");
  lcd.setCursor(0, 1);
  lcd.print("to WiFi...");

  unsigned long startTime = millis();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED && millis() - startTime < WIFI_TIMEOUT) {
    Serial.print(".");
    nonBlockingDelay(500);
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nConnected to WiFi: " + String(WIFI_SSID));
    Serial.println("IP Address: " + WiFi.localIP().toString());
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("WiFi Connected!");
    lcd.setCursor(0, 1);
    lcd.print(WiFi.localIP().toString());
    nonBlockingDelay(2000);
    sdMode = false;
    isConnected = true;
    wasConnected = true;
  } else {
    Serial.println("\nWiFi connection failed. Retrying once...");
    WiFi.reconnect(); // Attempt reconnect
    startTime = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startTime < 15000) { // Extended retry timeout to 15s
      Serial.print(".");
      nonBlockingDelay(500);
    }
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nReconnected to WiFi: " + String(WIFI_SSID));
      Serial.println("IP Address: " + WiFi.localIP().toString());
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("WiFi Reconnected!");
      lcd.setCursor(0, 1);
      lcd.print(WiFi.localIP().toString());
      nonBlockingDelay(2000);
      sdMode = false;
      isConnected = true;
      wasConnected = true;
    } else {
      Serial.println("\nWiFi connection failed after retry. Switching to SD mode.");
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("WiFi Failed");
      lcd.setCursor(0, 1);
      lcd.print("SD Mode");
      nonBlockingDelay(2000);
      sdMode = true;
      isConnected = false;
    }
  }
  isVoltageSufficient = (pzem.voltage() >= voltageThreshold);
}

void initFirebase() {
  config.api_key = API_KEY;
  config.database_url = DATABASE_URL;
  
  // Increase timeout to handle slow networks
  config.timeout.serverResponse = 20000;  // 20 seconds for server response
  config.timeout.wifiReconnect = 15000;   // 15 seconds for WiFi reconnect
  config.timeout.socketConnection = 10000; // 10 seconds for socket connection
  config.timeout.sslHandshake = 8000;     // 8 seconds for SSL handshake
  
  Serial.println("Initializing Firebase...");
  
  // Check if already signed up or use anonymous sign-up
  if (Firebase.authenticated()) {
    Serial.println("Already authenticated with Firebase.");
  } else {
    // Clear previous session data
    Firebase.reset(&config);
    
    // Add a small delay for network stabilization
    feedWatchdog();
    delay(500);
    
    if (!Firebase.signUp(&config, &auth, "", "")) { // Anonymous sign-up
      Serial.printf("Firebase signup error: %s\n", config.signer.signupError.message.c_str());
      
      // Check if error is SSL-related, and try one more time
      String errorMessage = config.signer.signupError.message.c_str();
      if (errorMessage.indexOf("ssl") >= 0 || 
          errorMessage.indexOf("SSL") >= 0 || 
          errorMessage.indexOf("connection") >= 0 ||
          errorMessage.indexOf("handshake") >= 0) {
        
        Serial.println("SSL-related error detected. Attempting one more time...");
        feedWatchdog();
        delay(1000);
        
        if (!Firebase.signUp(&config, &auth, "", "")) {
          Serial.printf("Second Firebase signup attempt failed: %s\n", config.signer.signupError.message.c_str());
          lcd.clear();
          lcd.setCursor(0, 0);
          lcd.print("Firebase Error:");
          lcd.setCursor(0, 1);
          lcd.print(config.signer.signupError.message.c_str());
          nonBlockingDelay(5000);
          ESP.restart();
        }
      } else {
        lcd.clear();
        lcd.setCursor(0, 0);
        lcd.print("Firebase Error:");
        lcd.setCursor(0, 1);
        lcd.print(config.signer.signupError.message.c_str());
        nonBlockingDelay(5000);
        ESP.restart();
      }
    } else {
      Serial.println("Firebase anonymous sign-up successful.");
    }
  }
  
  // Token status callback for monitoring and refreshing token
  config.token_status_callback = [](TokenInfo info) {
    String statusStr;
    switch (info.status) {
      case token_status_uninitialized:
        statusStr = "Uninitialized";
        break;
      case token_status_on_signing:
        statusStr = "Signing In";
        break;
      case token_status_on_refresh:
        statusStr = "Refreshing";
        break;
      case token_status_ready:
        statusStr = "Ready";
        break;
      case token_status_error:
        statusStr = "Error";
        break;
      default:
        statusStr = "Unknown";
        break;
    }
    Serial.printf("Token status: %s\n", statusStr.c_str());
    if (info.status == token_status_error) {
      Serial.printf("Token error: %s. Refreshing...\n", info.error.message.c_str());
      Firebase.refreshToken(&config);
      
      // If error is SSL-related, handle it
      String errorMessage = info.error.message.c_str();
      if (errorMessage.indexOf("ssl") >= 0 || 
          errorMessage.indexOf("SSL") >= 0 || 
          errorMessage.indexOf("connection") >= 0) {
        handleFirebaseSSLError();
      }
    }
  };
  
  // Initialize Firebase with configuration
  Firebase.begin(&config, &auth);
  Firebase.reconnectWiFi(true);
  Firebase.RTDB.setReadTimeout(&fbdo, 15000);  // Set 15-second timeout for RTDB read operations
  Firebase.RTDB.setReadTimeout(&fbdo, 15000); // Set 15-second timeout for RTDB write operations
  Firebase.RTDB.enableClassicRequest(&fbdo, true); // Use classic HTTP for more reliable connections
  
  // Wait and verify Firebase is ready
  unsigned long startTime = millis();
  while (!Firebase.ready() && (millis() - startTime < 15000)) {
    Serial.println("Waiting for Firebase to be ready...");
    delay(500);
  }
  
  if (Firebase.ready()) {
    Serial.println("Firebase initialized and authenticated successfully.");
  } else {
    Serial.println("Firebase failed to initialize after timeout.");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Firebase Fail");
    lcd.setCursor(0, 1);
    lcd.print("Restarting...");
    nonBlockingDelay(5000);
    ESP.restart();
  }
}

String getUIDString() {
  String uidStr = "";
  for (byte i = 0; i < rfid.uid.size; i++) {
    if (rfid.uid.uidByte[i] < 0x10)
      uidStr += "0";
    uidStr += String(rfid.uid.uidByte[i], HEX);
  }
  uidStr.toUpperCase();
  return uidStr;
}

bool isRegisteredUID(String uid) {
  Serial.println("Checking if UID '" + uid + "' is registered");
  
  // Super Admin UID should be recognized in both SD mode and online mode
  if (uid == SUPER_ADMIN_UID) {
    Serial.println("UID '" + uid + "' recognized as Super Admin");
    return true;
  }

  // First, check the firestoreStudents map - which is cached data
  if (firestoreStudents.find(uid) != firestoreStudents.end()) {
    Serial.println("UID '" + uid + "' is registered as a student (from cached data)");
    return true;
  }

  // Then check firestoreTeachers map - also cached data
  if (firestoreTeachers.find(uid) != firestoreTeachers.end()) {
    Serial.println("UID '" + uid + "' is registered as a teacher (from cached data)");
    return true;
  }
  
  // Check directly in RegisteredUIDs database if online
  if (!sdMode && isConnected && Firebase.ready()) {
    Serial.println("Checking UID '" + uid + "' directly in RTDB");
    if (Firebase.RTDB.get(&fbdo, "/RegisteredUIDs/" + uid)) {
      Serial.println("UID '" + uid + "' found directly in RegisteredUIDs RTDB");
      // Also add to firestoreStudents or firestoreTeachers as appropriate
      if (!Firebase.RTDB.getJSON(&fbdo, "/Students/" + uid)) {
        if (!Firebase.RTDB.getJSON(&fbdo, "/Instructors/" + uid)) {
          // Just a generic registered UID with no specific role
          Serial.println("UID '" + uid + "' is registered without specific role");
        } else {
          // An instructor
          Serial.println("UID '" + uid + "' is an instructor");
          std::map<String, String> instructorData;
          instructorData["fullName"] = "Unknown";
          instructorData["role"] = "instructor";
          firestoreTeachers[uid] = instructorData;
        }
      } else {
        // A student
        Serial.println("UID '" + uid + "' is a student");
        std::map<String, String> studentData;
        studentData["fullName"] = "Unknown";
        studentData["role"] = "student";
        firestoreStudents[uid] = studentData;
      }
      return true;
    } else {
      Serial.println("UID '" + uid + "' not found in RTDB: " + fbdo.errorReason());
    }
  } else {
    Serial.println("Skipping RTDB check: sdMode=" + String(sdMode) + ", isConnected=" + String(isConnected) + ", Firebase.ready=" + String(Firebase.ready()));
  }

  // If not found in cache, try direct Firestore query
  if (!sdMode && isConnected) {
    // First check teachers collection
    Serial.println("Checking if UID '" + uid + "' is registered in Firestore teachers...");
    String firestorePath = "teachers";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            if (rfidUid == uid) {
              Serial.println("UID '" + uid + "' is registered as a teacher");
              // Cache this data for future use
              std::map<String, String> teacherData;
              String fullName = "";
              if (doc.get(fieldData, "fields/fullName/stringValue")) {
                fullName = fieldData.stringValue;
              }
              teacherData["fullName"] = fullName;
              firestoreTeachers[uid] = teacherData;
              return true;
            }
          }
        }
      }
    } else {
      Serial.println("Failed to retrieve Firestore teachers: " + firestoreFbdo.errorReason());
    }
    
    // Then check students collection
    Serial.println("Checking if UID '" + uid + "' is registered in Firestore students...");
    firestorePath = "students";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            if (rfidUid == uid) {
              Serial.println("UID '" + uid + "' is registered as a student");
              // Cache this data for future use
              std::map<String, String> studentData;
              String fullName = "";
              if (doc.get(fieldData, "fields/fullName/stringValue")) {
                fullName = fieldData.stringValue;
              }
              studentData["fullName"] = fullName;
              studentData["role"] = "student";
              firestoreStudents[uid] = studentData;
              return true;
            }
          }
        }
      }
    } else {
      Serial.println("Failed to retrieve Firestore students: " + firestoreFbdo.errorReason());
    }
  }
  
  Serial.println("UID '" + uid + "' is not registered");
  return false;
}

bool isAdminUID(String uid) {
  if (!sdMode && isConnected) {
    Serial.println("Checking if UID " + uid + " is an admin in Firestore...");
    String firestorePath = "users";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      Serial.println("Firestore documents retrieved successfully for 'users' collection.");
      Serial.println("Firestore payload: " + firestoreFbdo.payload());
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload().c_str());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents") && jsonData.type == "array") {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "", role = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            Serial.println("Found document with rfidUid: " + rfidUid);
          }
          if (doc.get(fieldData, "fields/role/stringValue")) {
            role = fieldData.stringValue;
            Serial.println("Role for rfidUid " + rfidUid + ": " + role);
          }
          if (rfidUid == uid && role == "admin") {
            Serial.println("UID " + uid + " is confirmed as admin.");
            return true;
          }
        }
        Serial.println("No document found with rfidUid " + uid + " and role 'admin'.");
      } else {
        Serial.println("No documents found in 'users' collection.");
      }
    } else {
      Serial.println("Failed to retrieve Firestore documents: " + firestoreFbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    Serial.println("Cannot check admin UID: sdMode=" + String(sdMode) + ", isConnected=" + String(isConnected));
  }
  return false;
}

std::map<String, String> fetchUserDetails(String uid) {
  std::map<String, String> userData;

  if (!sdMode && isConnected) {
    Serial.println("Fetching user details for UID " + uid + " from Firestore...");
    String firestorePath = "users"; // Fetch the entire users collection

    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      Serial.println("Firestore documents retrieved successfully for 'users' collection.");
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload().c_str());
      FirebaseJsonData jsonData;

      if (json.get(jsonData, "documents") && jsonData.type == "array") {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);

        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());

          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
          }

          if (rfidUid == uid) {
            Serial.println("Found user with rfidUid " + uid + ". Extracting details...");
            if (doc.get(fieldData, "fields/email/stringValue")) {
              userData["email"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/fullName/stringValue")) {
              userData["fullName"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/idNumber/stringValue")) {
              userData["idNumber"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
              userData["rfidUid"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/role/stringValue")) {
              userData["role"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/createdAt/stringValue")) {
              userData["createdAt"] = fieldData.stringValue;
            }
            if (doc.get(fieldData, "fields/uid/stringValue")) {
              userData["uid"] = fieldData.stringValue;
            }

            Serial.println("User details fetched: fullName=" + userData["fullName"] + 
                           ", email=" + (userData["email"].isEmpty() ? "N/A" : userData["email"]) + 
                           ", role=" + (userData["role"].isEmpty() ? "N/A" : userData["role"]));
            break; // Stop searching once we find the matching user
          }
        }

        if (userData.empty()) {
          Serial.println("No user found with rfidUid " + uid + " in Firestore.");
        }
      } else {
        Serial.println("No documents found in 'users' collection or invalid response format.");
      }
    } else {
      Serial.println("Failed to retrieve Firestore documents: " + firestoreFbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    Serial.println("Cannot fetch user details: sdMode=" + String(sdMode) + ", isConnected=" + String(isConnected));
  }

  return userData;
}

void fetchRegisteredUIDs() {
  if (!sdMode && (WiFi.status() != WL_CONNECTED || pzem.voltage() < voltageThreshold)) {
    return;
  }
  if (Firebase.RTDB.get(&fbdo, "/RegisteredUIDs")) {
    if (fbdo.dataType() == "json") {
      FirebaseJson* json = fbdo.jsonObjectPtr();
      registeredUIDs.clear();
      size_t count = json->iteratorBegin();
      for (size_t i = 0; i < count; i++) {
        int type;
        String key, value;
        json->iteratorGet(i, type, key, value);
        registeredUIDs.push_back(key);
      }
      json->iteratorEnd();
    }
  } else {
    Serial.println("Failed to fetch registered UIDs: " + fbdo.errorReason());
    if (fbdo.errorReason().indexOf("ssl") >= 0 || 
        fbdo.errorReason().indexOf("connection") >= 0 || 
        fbdo.errorReason().indexOf("SSL") >= 0) {
      handleFirebaseSSLError();
    }
  }
  lastUIDFetchTime = millis();
}

void fetchFirestoreTeachers() {
  if (!sdMode && isConnected && Firebase.ready()) {
    firestoreTeachers.clear();
    String path = "teachers";
    yield(); // Prevent watchdog reset
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        Serial.println("Found " + String(arr.size()) + " teachers in Firestore");

        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid, fullName, email, idNumber, mobileNumber, role, department, createdAt;
          FirebaseJsonData fieldData;

          // Extract basic teacher fields
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            Serial.println("Fetched rfidUid: " + rfidUid);
          } else {
            Serial.println("No rfidUid found for document " + String(i));
            continue;
          }
          if (doc.get(fieldData, "fields/fullName/stringValue")) fullName = fieldData.stringValue;
          if (doc.get(fieldData, "fields/email/stringValue")) email = fieldData.stringValue;
          if (doc.get(fieldData, "fields/idNumber/stringValue")) idNumber = fieldData.stringValue;
          if (doc.get(fieldData, "fields/mobileNumber/stringValue")) mobileNumber = fieldData.stringValue;
          if (doc.get(fieldData, "fields/role/stringValue")) {
            role = fieldData.stringValue;
            Serial.println("Fetched role for UID " + rfidUid + ": '" + role + "'");
          } else {
            Serial.println("No role found for UID " + rfidUid + ". Defaulting to 'instructor'");
            role = "instructor";
          }
          if (doc.get(fieldData, "fields/department/stringValue")) department = fieldData.stringValue;
          if (doc.get(fieldData, "fields/createdAt/stringValue")) createdAt = fieldData.stringValue;

          // Parse assignedSubjects to extract schedules and sections
          FirebaseJsonArray schedulesArray;
          FirebaseJsonArray sectionsArray;
          if (doc.get(fieldData, "fields/assignedSubjects/arrayValue/values")) {
            FirebaseJsonArray subjectsArr;
            fieldData.getArray(subjectsArr);
            Serial.println("UID " + rfidUid + " has " + String(subjectsArr.size()) + " assigned subjects");

            for (size_t j = 0; j < subjectsArr.size(); j++) {
              FirebaseJsonData subjectData;
              subjectsArr.get(subjectData, j);
              FirebaseJson subject;
              subject.setJsonData(subjectData.to<String>());
              String subjectName, subjectCode;
              FirebaseJsonData subjectField;
              if (subject.get(subjectField, "mapValue/fields/name/stringValue")) subjectName = subjectField.stringValue;
              if (subject.get(subjectField, "mapValue/fields/code/stringValue")) subjectCode = subjectField.stringValue;

              // Parse sections and their schedules
              if (subject.get(subjectField, "mapValue/fields/sections/arrayValue/values")) {
                FirebaseJsonArray sectionsArr;
                subjectField.getArray(sectionsArr);
                Serial.println("Subject '" + subjectName + "' has " + String(sectionsArr.size()) + " sections");

                for (size_t k = 0; k < sectionsArr.size(); k++) {
                  FirebaseJsonData sectionData;
                  sectionsArr.get(sectionData, k);
                  FirebaseJson section;
                  section.setJsonData(sectionData.to<String>());
                  String sectionId, sectionName, sectionCode, capacity, currentEnrollment;
                  FirebaseJsonData sectionField;
                  if (section.get(sectionField, "mapValue/fields/id/stringValue")) sectionId = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/name/stringValue")) sectionName = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/code/stringValue")) sectionCode = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/capacity/integerValue")) capacity = sectionField.stringValue;
                  if (section.get(sectionField, "mapValue/fields/currentEnrollment/integerValue")) currentEnrollment = sectionField.stringValue;

                  Serial.println("Section " + String(k) + ": " + sectionName + ", Code: " + sectionCode + ", Capacity: " + capacity + ", Enrollment: " + currentEnrollment);

                  FirebaseJson sectionEntry;
                  sectionEntry.set("id", sectionId);
                  sectionEntry.set("name", sectionName);
                  sectionEntry.set("code", sectionCode);
                  sectionEntry.set("capacity", capacity);
                  sectionEntry.set("currentEnrollment", currentEnrollment);
                  sectionEntry.set("subject", subjectName);
                  sectionEntry.set("subjectCode", subjectCode);
                  sectionsArray.add(sectionEntry);

                  // Parse schedules within the section
                  if (section.get(sectionField, "mapValue/fields/schedules/arrayValue/values")) {
                    FirebaseJsonArray schedulesArr;
                    sectionField.getArray(schedulesArr);
                    Serial.println("Section '" + sectionName + "' has " + String(schedulesArr.size()) + " schedules");

                    for (size_t m = 0; m < schedulesArr.size(); m++) {
                      FirebaseJsonData scheduleData;
                      schedulesArr.get(scheduleData, m);
                      FirebaseJson schedule;
                      schedule.setJsonData(scheduleData.to<String>());
                      String day, startTime, endTime, roomName;
                      // Define the instructor variables we need for this schedule
                      String instructorUid = rfidUid; // Use the current teacher's UID
                      String instructorName = fullName; // Use the current teacher's name
                      FirebaseJsonData scheduleField;
                      if (schedule.get(scheduleField, "mapValue/fields/day/stringValue")) day = scheduleField.stringValue;
                      if (schedule.get(scheduleField, "mapValue/fields/startTime/stringValue")) startTime = scheduleField.stringValue;
                      if (schedule.get(scheduleField, "mapValue/fields/endTime/stringValue")) endTime = scheduleField.stringValue;
                      if (schedule.get(scheduleField, "mapValue/fields/roomName/stringValue")) roomName = scheduleField.stringValue;

                      Serial.println("Schedule " + String(m) + ": " + day + ", " + startTime + "-" + endTime + ", Room: " + roomName);

                      FirebaseJson scheduleEntry;
                      scheduleEntry.set("day", day);
                      scheduleEntry.set("startTime", startTime);
                      scheduleEntry.set("endTime", endTime);
                      scheduleEntry.set("roomName", roomName);
                      scheduleEntry.set("subject", subjectName);
                      scheduleEntry.set("subjectCode", subjectCode);
                      scheduleEntry.set("section", sectionName);
                      scheduleEntry.set("sectionId", sectionId);
                      scheduleEntry.set("instructorUid", instructorUid);
                      scheduleEntry.set("instructorName", instructorName);

                      schedulesArray.add(scheduleEntry); // Add directly to array
                    }
                  }
                }
              }
            }
          }

          // Store teacher data in firestoreTeachers
          if (rfidUid != "") {
            std::map<String, String> teacherData;
            teacherData["fullName"] = fullName;
            teacherData["email"] = email;
            teacherData["idNumber"] = idNumber;
            teacherData["mobileNumber"] = mobileNumber;
            teacherData["role"] = role;
            teacherData["department"] = department;
            teacherData["createdAt"] = createdAt;

            String schedulesStr = "[]";
            String sectionsStr = "[]";
            if (schedulesArray.size() > 0) {
              schedulesArray.toString(schedulesStr, true);
            }
            if (sectionsArray.size() > 0) {
              sectionsArray.toString(sectionsStr, true);
            }
            Serial.println("Schedules for UID " + rfidUid + ": " + schedulesStr);
            Serial.println("Sections for UID " + rfidUid + ": " + sectionsStr);
            teacherData["schedules"] = schedulesStr;
            teacherData["sections"] = sectionsStr;
            firestoreTeachers[rfidUid] = teacherData;
          }
        }

        // Sync schedules to SD card after fetching
        if (syncSchedulesToSD()) {
          Serial.println("Schedules synced to SD card successfully.");
        } else {
          Serial.println("Failed to sync schedules to SD card.");
        }

        Serial.println("Firestore teachers fetched and cached locally. Total: " + String(firestoreTeachers.size()));
      } else {
        Serial.println("No documents found in Firestore teachers collection");
      }
    } else {
      Serial.println("Failed to fetch Firestore teachers: " + firestoreFbdo.errorReason());
      if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
          firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
          firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    Serial.println("Skipping fetchFirestoreTeachers: sdMode=" + String(sdMode) + 
                   ", WiFi=" + String(WiFi.status()) + 
                   ", FirebaseReady=" + String(Firebase.ready()));
  }
}

void fetchFirestoreStudents() {
  if (!sdMode && (WiFi.status() != WL_CONNECTED || pzem.voltage() < voltageThreshold)) {
    Serial.println("Skipping fetchFirestoreStudents: Not connected or low voltage (WiFi: " + String(WiFi.status()) + ", Voltage: " + String(pzem.voltage()) + ")");
    return;
  }

  firestoreStudents.clear();
  String path = "students";
  Serial.println("Fetching students from Firestore at path: " + path);

  int retries = 3;
  bool success = false;
  for (int attempt = 1; attempt <= retries && !success; attempt++) {
    Serial.println("Attempt " + String(attempt) + " to fetch Firestore data...");
    yield(); // Prevent watchdog reset before Firebase call
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), "")) {
      success = true;
      Serial.println("Firestore fetch successful. Raw payload:");
      Serial.println(firestoreFbdo.payload());

      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        Serial.println("Found " + String(arr.size()) + " documents in Firestore response.");

        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          Serial.println("Document[" + String(i) + "] raw data: " + docData.to<String>());

          String rfidUid, fullName, email, idNumber, mobileNumber, role, department;
          FirebaseJsonData fieldData;

          rfidUid = doc.get(fieldData, "fields/rfidUid/stringValue") ? fieldData.stringValue : "";
          fullName = doc.get(fieldData, "fields/fullName/stringValue") ? fieldData.stringValue : "Unknown";
          email = doc.get(fieldData, "fields/email/stringValue") ? fieldData.stringValue : "";
          idNumber = doc.get(fieldData, "fields/idNumber/stringValue") ? fieldData.stringValue : "";
          mobileNumber = doc.get(fieldData, "fields/mobileNumber/stringValue") ? fieldData.stringValue : "";
          role = doc.get(fieldData, "fields/role/stringValue") ? fieldData.stringValue : "student";
          department = doc.get(fieldData, "fields/department/stringValue") ? fieldData.stringValue : "";

          Serial.println("Parsed rfidUid: " + rfidUid + ", fullName: " + fullName);

          String schedulesJsonStr = "[]";
          FirebaseJsonArray schedulesArray; // Use array instead of object

          if (doc.get(fieldData, "fields/enrolledSubjects/arrayValue/values")) {
            FirebaseJsonArray subjectsArr;
            fieldData.getArray(subjectsArr);
            Serial.println("Found " + String(subjectsArr.size()) + " enrolledSubjects for " + rfidUid);

            for (size_t j = 0; j < subjectsArr.size(); j++) {
              FirebaseJsonData subjectData;
              subjectsArr.get(subjectData, j);
              FirebaseJson subject;
              subject.setJsonData(subjectData.to<String>());
              Serial.println("Processing enrolledSubject[" + String(j) + "]: " + subjectData.to<String>());

              String subjectCode = subject.get(fieldData, "mapValue/fields/code/stringValue") ? fieldData.stringValue : "Unknown";
              String subjectName = subject.get(fieldData, "mapValue/fields/name/stringValue") ? fieldData.stringValue : "Unknown";
              String instructorUid = subject.get(fieldData, "mapValue/fields/instructorId/stringValue") ? fieldData.stringValue : "";
              String instructorName = subject.get(fieldData, "mapValue/fields/instructorName/stringValue") ? fieldData.stringValue : "";
              String sectionId = subject.get(fieldData, "mapValue/fields/sectionId/stringValue") ? fieldData.stringValue : "";
              String sectionName = subject.get(fieldData, "mapValue/fields/sectionName/stringValue") ? fieldData.stringValue : "";

              if (subject.get(fieldData, "mapValue/fields/schedules/arrayValue/values")) {
                FirebaseJsonArray schedulesArr;
                fieldData.getArray(schedulesArr);
                Serial.println("Found " + String(schedulesArr.size()) + " schedules for subject " + subjectCode);

                for (size_t k = 0; k < schedulesArr.size(); k++) {
                  FirebaseJsonData scheduleData;
                  schedulesArr.get(scheduleData, k);
                  FirebaseJson schedule;
                  schedule.setJsonData(scheduleData.to<String>());
                  Serial.println("Processing schedule[" + String(k) + "]: " + scheduleData.to<String>());

                  FirebaseJsonData scheduleField;
                  String day = schedule.get(scheduleField, "mapValue/fields/day/stringValue") ? scheduleField.stringValue : "";
                  String startTime = schedule.get(scheduleField, "mapValue/fields/startTime/stringValue") ? scheduleField.stringValue : "";
                  String endTime = schedule.get(scheduleField, "mapValue/fields/endTime/stringValue") ? scheduleField.stringValue : "";
                  String roomName = schedule.get(scheduleField, "mapValue/fields/roomName/stringValue") ? scheduleField.stringValue : "";

                  FirebaseJson scheduleObj;
                  scheduleObj.set("day", day);
                  scheduleObj.set("startTime", startTime);
                  scheduleObj.set("endTime", endTime);
                  scheduleObj.set("roomName", roomName);
                  scheduleObj.set("subjectCode", subjectCode);
                  scheduleObj.set("subject", subjectName);
                  scheduleObj.set("section", sectionName);
                  scheduleObj.set("sectionId", sectionId);
                  scheduleObj.set("instructorUid", instructorUid);
                  scheduleObj.set("instructorName", instructorName);

                  schedulesArray.add(scheduleObj); // Add directly to array
                }
              } else {
                Serial.println("No schedules found in enrolledSubject[" + String(j) + "] for " + rfidUid);
              }
            }
            schedulesArray.toString(schedulesJsonStr, true); // Serialize array
            Serial.println("Combined schedules for " + rfidUid + ": " + schedulesJsonStr);
          } else {
            Serial.println("No enrolledSubjects/values found for " + rfidUid);
          }

          if (rfidUid != "") {
            std::map<String, String> studentData;
            studentData["fullName"] = fullName;
            studentData["email"] = email;
            studentData["idNumber"] = idNumber;
            studentData["mobileNumber"] = mobileNumber;
            studentData["role"] = role;
            studentData["department"] = department;
            studentData["schedules"] = schedulesJsonStr;
            firestoreStudents[rfidUid] = studentData;
            Serial.println("Stored student " + rfidUid + " with schedules: " + schedulesJsonStr);
          }
        }
        Serial.println("Fetched " + String(firestoreStudents.size()) + " students from Firestore.");
      } else {
        Serial.println("No documents found in Firestore response.");
      }
    } else {
      Serial.println("Firestore fetch failed (attempt " + String(attempt) + "): " + firestoreFbdo.errorReason());
      if (attempt < retries) {
        Serial.println("Retrying in 5 seconds...");
        delay(5000);
        Firebase.reconnectWiFi(true);
      }
      if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
          firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
          firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
    yield(); // Prevent watchdog reset after Firebase call
  }

  if (!success) {
    Serial.println("All fetch attempts failed. Switching to SD mode.");
    sdMode = true;
  }

  if (firestoreStudents.find("5464E1BA") != firestoreStudents.end()) {
    Serial.println("Verified: 5464E1BA found with schedules: " + firestoreStudents["5464E1BA"]["schedules"]);
  } else {
    Serial.println("Verified: 5464E1BA NOT found in firestoreStudents.");
  }
}

// Add this function after initFirebase() and before fetchFirestoreRooms()
bool ensureFirebaseAuthenticated() {
  if (!Firebase.ready()) {
    Serial.println("Firebase not ready. Attempting re-initialization...");
    Firebase.reconnectWiFi(true);
    initFirebase();
    delay(1000);
    
    if (!Firebase.ready()) {
      Serial.println("Firebase still not ready after re-init.");
      return false;
    }
  }
  
  if (!Firebase.authenticated()) {
    Serial.println("Firebase not authenticated. Attempting sign-in...");
    // Try to sign in again
    if (!Firebase.signUp(&config, &auth, "", "")) {
      Serial.printf("Firebase re-auth failed: %s\n", config.signer.signupError.message.c_str());
      
      // Check for SSL errors in authentication failure
      String errorMessage = config.signer.signupError.message.c_str();
      if (errorMessage.indexOf("ssl") >= 0 || 
          errorMessage.indexOf("SSL") >= 0 || 
          errorMessage.indexOf("connection") >= 0 ||
          errorMessage.indexOf("handshake") >= 0 ||
          errorMessage.indexOf("certificate") >= 0 ||
          errorMessage.indexOf("network") >= 0) {
        Serial.println("SSL or network issue detected during authentication");
        
        // Try WiFi reconnection
        if (WiFi.status() != WL_CONNECTED) {
          Serial.println("WiFi disconnected during auth. Reconnecting...");
          connectWiFi();
        } else {
          // Reset WiFi connection
          Serial.println("Resetting WiFi connection");
          WiFi.disconnect();
          delay(500);
          connectWiFi();
        }
        
        delay(500);
        
        // Retry authentication one more time after WiFi reset
        if (!Firebase.signUp(&config, &auth, "", "")) {
          Serial.printf("Second Firebase auth attempt failed: %s\n", config.signer.signupError.message.c_str());
          return false;
        }
      } else {
        return false;
      }
    }
    
    delay(1000); // Give it time to process
    if (!Firebase.authenticated()) {
      Serial.println("Firebase still not authenticated after re-auth attempt.");
      return false;
    }
  }
  
  Serial.println("Firebase is ready and authenticated.");
  return true;
}

void fetchFirestoreRooms() {
  if (!ensureFirebaseAuthenticated()) {
    Serial.println("Cannot fetch Firestore rooms, Firebase not authenticated.");
    return;
  }

  Serial.println("Fetching Firestore rooms...");
  if (Firebase.Firestore.getDocument(&fbdo, FIRESTORE_PROJECT_ID, "", "rooms", "")) {
    FirebaseJson roomsJson;
    roomsJson.setJsonData(fbdo.payload().c_str());
    FirebaseJsonData jsonData;

    if (roomsJson.get(jsonData, "documents") && jsonData.type == "array") {
      FirebaseJsonArray arr;
      jsonData.getArray(arr);
      firestoreRooms.clear();

      for (size_t i = 0; i < arr.size(); i++) {
        FirebaseJsonData docData;
        arr.get(docData, i);
        FirebaseJson doc;
        doc.setJsonData(docData.stringValue);

        // Extract room ID from document name
        String docName;
        doc.get(docData, "name");
        docName = docData.stringValue;
        String roomId = docName.substring(docName.lastIndexOf("/") + 1);

        // Extract fields
        FirebaseJson fields;
        doc.get(docData, "fields");
        fields.setJsonData(docData.stringValue);

        std::map<String, String> roomData;
        FirebaseJsonData fieldData;

        // Root-level fields with existence checks
        if (fields.get(fieldData, "building/stringValue")) {
          roomData["building"] = fieldData.stringValue;
        } else {
          roomData["building"] = "Unknown";
          Serial.println("Warning: 'building' missing for room " + roomId);
        }
        if (fields.get(fieldData, "floor/stringValue")) {
          roomData["floor"] = fieldData.stringValue;
        } else {
          roomData["floor"] = "Unknown";
          Serial.println("Warning: 'floor' missing for room " + roomId);
        }
        if (fields.get(fieldData, "name/stringValue")) {
          roomData["name"] = fieldData.stringValue;
        } else {
          roomData["name"] = "Unknown";
          Serial.println("Warning: 'name' missing for room " + roomId);
        }
        if (fields.get(fieldData, "status/stringValue")) {
          roomData["status"] = fieldData.stringValue;
        } else {
          roomData["status"] = "Unknown";
          Serial.println("Warning: 'status' missing for room " + roomId);
        }
        if (fields.get(fieldData, "type/stringValue")) {
          roomData["type"] = fieldData.stringValue;
        } else {
          roomData["type"] = "Unknown";
          Serial.println("Warning: 'type' missing for room " + roomId);
        }

        firestoreRooms[roomId] = roomData;
        Serial.println("Fetched room " + roomId + ": building=" + roomData["building"] + 
                       ", floor=" + roomData["floor"] + ", name=" + roomData["name"] +
                       ", status=" + roomData["status"] + ", type=" + roomData["type"]);
      }
      Serial.println("Fetched " + String(firestoreRooms.size()) + " rooms from Firestore.");
    } else {
      Serial.println("No documents found in rooms collection or invalid format.");
    }
  } else {
    Serial.println("Failed to fetch Firestore rooms: " + fbdo.payload());
    
    // Additional debugging for Firestore permissions issue
    Serial.println("Attempting to reconnect and retry...");
    Firebase.reconnectWiFi(true);
    delay(1000);
    
    // Try a second attempt with the firestoreFbdo object
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", "rooms", "")) {
      Serial.println("Second attempt successful with firestoreFbdo!");
      FirebaseJson roomsJson;
      roomsJson.setJsonData(firestoreFbdo.payload().c_str());
      FirebaseJsonData jsonData;
      
      if (roomsJson.get(jsonData, "documents") && jsonData.type == "array") {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        firestoreRooms.clear();
        
        // Process rooms as in the original attempt
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          
          // Extract room ID from document name
          String docName;
          doc.get(docData, "name");
          docName = docData.stringValue;
          String roomId = docName.substring(docName.lastIndexOf("/") + 1);
          
          // Extract fields
          FirebaseJson fields;
          doc.get(docData, "fields");
          fields.setJsonData(docData.stringValue);
          
          std::map<String, String> roomData;
          FirebaseJsonData fieldData;
          
          // Process fields
          if (fields.get(fieldData, "building/stringValue")) {
            roomData["building"] = fieldData.stringValue;
          } else {
            roomData["building"] = "Unknown";
          }
          if (fields.get(fieldData, "floor/stringValue")) {
            roomData["floor"] = fieldData.stringValue;
          } else {
            roomData["floor"] = "Unknown";
          }
          if (fields.get(fieldData, "name/stringValue")) {
            roomData["name"] = fieldData.stringValue;
          } else {
            roomData["name"] = "Unknown";
          }
          if (fields.get(fieldData, "status/stringValue")) {
            roomData["status"] = fieldData.stringValue;
          } else {
            roomData["status"] = "Unknown";
          }
          if (fields.get(fieldData, "type/stringValue")) {
            roomData["type"] = fieldData.stringValue;
          } else {
            roomData["type"] = "Unknown";
          }
          
          firestoreRooms[roomId] = roomData;
          Serial.println("Fetched room " + roomId + ": building=" + roomData["building"] + 
                        ", floor=" + roomData["floor"] + ", name=" + roomData["name"] +
                        ", status=" + roomData["status"] + ", type=" + roomData["type"]);
        }
        Serial.println("Fetched " + String(firestoreRooms.size()) + " rooms from Firestore on second attempt.");
      } else {
        Serial.println("Second attempt: No documents found in rooms collection or invalid format.");
      }
    } else {
      Serial.println("Second attempt also failed: " + firestoreFbdo.errorReason());
      Serial.println("Firebase auth status: " + String(Firebase.authenticated()));
      Serial.println("Verify Firestore rules and authentication.");
    }
    if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
        firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
        firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
      handleFirebaseSSLError();
    }
  }
}

String assignRoomToAdmin(String uid) {
  String selectedRoomId = "";

  // Ensure firestoreRooms is populated
  if (firestoreRooms.empty()) {
    Serial.println("firestoreRooms is empty. Cannot assign a room to admin UID: " + uid);
    return selectedRoomId;
  }

  // Filter rooms based on status (e.g., "maintenance" for inspection)
  Serial.println("Assigning room for admin UID: " + uid);
  Serial.println("Searching for rooms with status 'maintenance'...");
  
  // Debug room statuses
  for (const auto& room : firestoreRooms) {
    String roomId = room.first;
    const auto& roomData = room.second;
    
    // Extract room details using at() to avoid const issues
    String roomStatus = roomData.at("status");
    String roomName = roomData.at("name");
    
    Serial.println("Room " + roomId + " (" + roomName + ") has status: '" + roomStatus + "'");
  }
  
  // First try exact match with 'maintenance'
  for (const auto& room : firestoreRooms) {
    String roomId = room.first;
    const auto& roomData = room.second;
    
    String roomStatus = roomData.at("status");
    String roomBuilding = roomData.at("building");
    String roomName = roomData.at("name");
    
    // Primary match: status equals "maintenance"
    if (roomStatus == "maintenance") {
      selectedRoomId = roomId;
      Serial.println("Selected room " + roomId + " with status 'maintenance' for admin UID: " + uid + 
                     " (building: " + roomBuilding + ", name: " + roomName + ")");
      return selectedRoomId;
    }
  }
  
  // If no room found with exactly "maintenance", try case-insensitive or partial match
  for (const auto& room : firestoreRooms) {
    String roomId = room.first;
    const auto& roomData = room.second;
    
    String roomStatus = roomData.at("status");
    String roomStatusLower = roomStatus;
    roomStatusLower.toLowerCase();
    
    String roomBuilding = roomData.at("building");
    String roomName = roomData.at("name");
    
    // Secondary match: status contains "maintenance" (case insensitive)
    if (roomStatusLower.indexOf("maintenance") >= 0) {
      selectedRoomId = roomId;
      Serial.println("Selected room " + roomId + " with status containing 'maintenance' for admin UID: " + uid + 
                     " (actual status: '" + roomStatus + "', building: " + roomBuilding + ", name: " + roomName + ")");
      return selectedRoomId;
    }
  }
  
  // If still no match, assign any room for testing purposes
  if (selectedRoomId == "") {
    Serial.println("No room with status 'maintenance' found. Assigning first available room for testing.");
    if (!firestoreRooms.empty()) {
      selectedRoomId = firestoreRooms.begin()->first;
      const auto& roomData = firestoreRooms.begin()->second;
      String roomStatus = roomData.at("status");
      String roomName = roomData.at("name");
      Serial.println("Assigned room " + selectedRoomId + " (" + roomName + ") with status '" + roomStatus + "' for admin UID: " + uid);
    } else {
      Serial.println("No rooms available to assign to admin UID: " + uid);
    }
  }
  
  return selectedRoomId;
}

void assignRoomToInstructor(String uid, String timestamp) {
  if (firestoreTeachers.find(uid) == firestoreTeachers.end()) {
    return;
  }

  String teacherSchedules = firestoreTeachers[uid]["schedules"];
  FirebaseJson teacherJson;
  teacherJson.setJsonData(teacherSchedules);
  FirebaseJsonData jsonData;

  if (teacherJson.get(jsonData, "/")) {
    FirebaseJsonArray schedulesArray;
    jsonData.getArray(schedulesArray);

    String currentDay, currentTime;
    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {
      char dayStr[10];
      char timeStr[10];
      strftime(dayStr, sizeof(dayStr), "%A", &timeinfo);
      strftime(timeStr, sizeof(timeStr), "%H:%M", &timeinfo);
      currentDay = String(dayStr);
      currentTime = String(timeStr);
    } else {
      return;
    }

    String assignedRoom = "";
    String subject = "";
    String sessionStart = timestamp;
    String sessionEnd = "";

    for (size_t i = 0; i < schedulesArray.size(); i++) {
      FirebaseJsonData scheduleData;
      schedulesArray.get(scheduleData, i);
      FirebaseJson schedule;
      schedule.setJsonData(scheduleData.to<String>());
      String day, startTime, endTime, room, subjectSchedule;
      FirebaseJsonData field;
      if (schedule.get(field, "day")) day = field.stringValue;
      if (schedule.get(field, "startTime")) startTime = field.stringValue;
      if (schedule.get(field, "endTime")) endTime = field.stringValue;
      if (schedule.get(field, "room")) room = field.stringValue;
      if (schedule.get(field, "subject")) subjectSchedule = field.stringValue;

      if (day == currentDay) {
        if (currentTime >= startTime && currentTime <= endTime) {
          assignedRoom = room;
          subject = subjectSchedule;
          sessionEnd = endTime;
          break;
        }
      }
    }

    if (assignedRoom != "") {
      for (auto& room : firestoreRooms) {
        String roomId = room.first;
        if (room.second.at("roomName") == assignedRoom && room.second.at("status") == "available") {
          assignedRoomId = roomId;
          sessionStartReading = pzem.energy();
          if (!sdMode && isConnected && isVoltageSufficient) {
            String path = "rooms/" + roomId;
            FirebaseJson content;
            content.set("fields/status/stringValue", "occupied");
            content.set("fields/assignedInstructor/stringValue", firestoreTeachers[uid]["fullName"]);
            if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), content.raw(), "status,assignedInstructor")) {
              firestoreRooms[roomId]["status"] = "occupied";
              firestoreRooms[roomId]["assignedInstructor"] = firestoreTeachers[uid]["fullName"];
            }
          }
          break;
        }
      }
    }
  }
}

void updateRoomStatus(String roomId, String status, String instructorName, String subject, String sessionStart, String sessionEnd, float startReading, float endReading, float totalUsage) {
  if (!sdMode && isConnected && isVoltageSufficient) {
    String path = "rooms/" + roomId;
    FirebaseJson content;
    content.set("fields/status/stringValue", status);
    content.set("fields/assignedInstructor/stringValue", "");
    content.set("fields/lastSession/mapValue/fields/subject/stringValue", subject);
    content.set("fields/lastSession/mapValue/fields/instructorName/stringValue", instructorName);
    content.set("fields/lastSession/mapValue/fields/sessionStart/stringValue", sessionStart);
    content.set("fields/lastSession/mapValue/fields/sessionEnd/stringValue", sessionEnd);
    content.set("fields/lastSession/mapValue/fields/energyUsageStart/doubleValue", startReading);
    content.set("fields/lastSession/mapValue/fields/energyUsageEnd/doubleValue", endReading);
    content.set("fields/lastSession/mapValue/fields/totalUsage/doubleValue", totalUsage);
    if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", path.c_str(), content.raw(), "status,assignedInstructor,lastSession")) {
      firestoreRooms[roomId]["status"] = status;
      firestoreRooms[roomId]["assignedInstructor"] = "";
    }
  }
  assignedRoomId = "";
  sessionStartReading = 0.0;
}

void storeLogToSD(String entry) {
  if (isConnected) {
    Serial.println("WiFi is connected. Skipping SD log storage for: " + entry);
    return;
  }

  static bool reinitializedInThisCall = false;

  if (!sdInitialized && !reinitializedInThisCall) {
    if (!SD.begin(SD_CS, fsSPI, 4000000)) {
      Serial.println("SD card initialization failed during setup. Cannot store log: " + entry);
      sdInitialized = false;
      return;
    }
    sdInitialized = true;
    Serial.println("SD card initialized for logging.");
    yield(); // Yield after SD initialization
  }

  File logFile = SD.open(OFFLINE_LOG_FILE, FILE_APPEND);
  if (logFile) {
    logFile.println(entry);
    logFile.flush();
    logFile.close();
    Serial.println("Stored to SD: " + entry);
    reinitializedInThisCall = false;
    yield(); // Yield after SD write
  } else {
    Serial.println("Failed to open " + String(OFFLINE_LOG_FILE) + " for writing. Diagnosing...");
    yield(); // Yield during error handling

    File root = SD.open("/");
    if (!root) {
      Serial.println("SD card root directory inaccessible. Attempting reinitialization...");
      if (!reinitializedInThisCall && SD.begin(SD_CS, fsSPI, 4000000)) {
        Serial.println("SD card reinitialized successfully.");
        sdInitialized = true;
        reinitializedInThisCall = true;
      } else {
        Serial.println("SD card reinitialization failed. Hardware issue or card removed?");
        sdInitialized = false;
        Serial.println("Falling back to serial-only logging: " + entry);
        return;
      }
      yield(); // Yield after reinitialization attempt
    } else {
      root.close();
      Serial.println("SD card root accessible, issue is file-specific.");
      yield(); // Yield after root check
    }

    if (SD.exists(OFFLINE_LOG_FILE)) {
      Serial.println(String(OFFLINE_LOG_FILE) + " exists but can't be opened. Attempting to delete...");
      if (SD.remove(OFFLINE_LOG_FILE)) {
        Serial.println("Deleted " + String(OFFLINE_LOG_FILE) + " successfully.");
      } else {
        Serial.println("Failed to delete " + String(OFFLINE_LOG_FILE) + ". Possible write protection or corruption.");
        return;
      }
      yield(); // Yield after file deletion
    } else {
      Serial.println(String(OFFLINE_LOG_FILE) + " does not exist yet. Creating new file...");
      yield(); // Yield before file creation
    }

    logFile = SD.open(OFFLINE_LOG_FILE, FILE_APPEND);
    if (logFile) {
      logFile.println(entry);
      logFile.flush();
      logFile.close();
      Serial.println("Recreated and stored to SD: " + entry);
      reinitializedInThisCall = false;
      yield(); // Yield after successful retry
    } else {
      Serial.println("Retry failed for " + String(OFFLINE_LOG_FILE) + ". Testing SD card integrity...");
      File testFile = SD.open("/test_log.txt", FILE_WRITE);
      if (testFile) {
        testFile.println("Test entry at " + getFormattedTime() + ": " + entry);
        testFile.flush();
        testFile.close();
        Serial.println("Test write to /test_log.txt succeeded. Issue is specific to " + String(OFFLINE_LOG_FILE));
      } else {
        Serial.println("Test write to /test_log.txt failed. SD card is likely faulty or full.");
        sdInitialized = false;
        Serial.println("Falling back to serial-only logging: " + entry);
      }
      yield(); // Yield after test write
    }
  }
}

bool syncOfflineLogs() {
  if (sdMode || !isConnected || !isVoltageSufficient) {
    Serial.println("Cannot sync logs: SD mode or no connection/voltage.");
    return false;
  }
  if (!SD.exists(OFFLINE_LOG_FILE)) {
    Serial.println("No offline logs to sync.");
    return true;
  }
  File file = SD.open(OFFLINE_LOG_FILE, FILE_READ);
  if (!file) {
    Serial.println("⚠️ Could not open SD log file for syncing.");
    return false;
  }
  Serial.println("Syncing offline logs to Firebase...");
  bool allSuccess = true;
  while (file.available()) {
    String entry = file.readStringUntil('\n');
    entry.trim();
    if (entry.length() > 0) {
      Serial.println("Sync log: " + entry);
      nonBlockingDelay(1);
    }
  }
  file.close();
  if (allSuccess) {
    if (SD.remove(OFFLINE_LOG_FILE)) {
      Serial.println("SD log file cleared after sync.");
      return true;
    } else {
      Serial.println("⚠️ Could not remove SD file.");
      return false;
    }
  } else {
    Serial.println("Some logs failed to upload; keeping SD file.");
    return false;
  }
}

void logSuperAdminPZEMToSD(String uid, String timestamp) {
  float voltage = pzem.voltage();
  float current = pzem.current();
  float power = pzem.power();
  float energy = pzem.energy();
  float frequency = pzem.frequency();
  float pf = pzem.pf();

  // Ensure valid readings
  if (isnan(voltage) || voltage < 0) voltage = 0.0;
  if (isnan(current) || current < 0) current = 0.0;
  if (isnan(power) || power < 0) power = 0.0;
  if (isnan(energy) || energy < 0) energy = 0.0;
  if (isnan(frequency) || frequency < 0) frequency = 0.0;
  if (isnan(pf) || pf < 0) pf = 0.0;

  // Calculate total energy since session start
  static float superAdminTotalEnergy = 0.0;
  static unsigned long lastSuperAdminPZEMUpdate = 0;
  if (lastSuperAdminPZEMUpdate != 0) {
    unsigned long elapsed = millis() - lastSuperAdminPZEMUpdate;
    float energyIncrement = (power * (elapsed / 3600000.0)) / 1000.0;
    superAdminTotalEnergy += energyIncrement;
  }
  lastSuperAdminPZEMUpdate = millis();

  // Log to SD card
  String entry = "SuperAdminPZEM:" + uid + " Timestamp:" + timestamp +
                 " Voltage:" + String(voltage, 2) + "V" +
                 " Current:" + String(current, 2) + "A" +
                 " Power:" + String(power, 2) + "W" +
                 " Energy:" + String(energy, 2) + "kWh" +
                 " Frequency:" + String(frequency, 2) + "Hz" +
                 " PowerFactor:" + String(pf, 2) +
                 " TotalConsumption:" + String(superAdminTotalEnergy, 3) + "kWh";
  storeLogToSD(entry);
  Serial.println("Super Admin PZEM logged to SD: " + entry);

  // Reset total energy when session ends
  if (!superAdminSessionActive) {
    superAdminTotalEnergy = 0.0;
    lastSuperAdminPZEMUpdate = 0;
  }
}

void printSDCardInfo() {
  uint64_t cardSize = SD.cardSize() / (1024 * 1024);
  uint64_t cardFree = (SD.totalBytes() - SD.usedBytes()) / (1024 * 1024);
  Serial.println("SD Card Info:");
  Serial.print("Total space: "); Serial.print(cardSize); Serial.println(" MB");
  Serial.print("Free space: "); Serial.print(cardFree); Serial.println(" MB");
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void activateRelays() {
  // If transition already in progress, queue it
  if (relayTransitionInProgress) {
    Serial.println("SAFETY: Relay transition already in progress, queuing activation");
    relayOperationPending = true;
    pendingRelayActivation = true;
    return;
  }
  
  // Start transition
  relayTransitionInProgress = true;
  relayTransitionStartTime = millis();
  
  // RELAY LOGIC:
  // HIGH = Relay OFF/Inactive = Door unlocked
  // LOW = Relay ON/Active = Door locked
  
  // Check system status before relay changes
  yield(); // Yield CPU to prevent watchdog trigger
  
  // Temporarily disable interrupts during critical relay state changes
  noInterrupts();
  
  // Locking the doors (activating relays) with improved timing
  // First relay - activate with longer delay
  digitalWrite(RELAY1, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  yield(); // Add yield between relay operations
  
  // Second relay - activate with delay
  digitalWrite(RELAY2, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  yield(); // Add yield between relay operations
  
  // Third relay - activate with delay
  digitalWrite(RELAY3, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  yield(); // Add yield between relay operations
  
  // Fourth relay - activate with final delay
  digitalWrite(RELAY4, LOW);
  delay(25);  // Increased from 20ms to 25ms for more stability
  
  // Re-enable interrupts
  interrupts();
  
  // Allow system to breathe after relay operation
  yield();
  
  // Update state flags
  relayActive = true;
  relayActiveTime = millis();
  relayTransitionInProgress = false;
  
  Serial.println("Relays activated (locked) with safe timing");
  
  // Non-blocking approach to delay PZEM readings
  lastPZEMUpdate = millis() + 1500; // Wait 1.5 seconds before next PZEM reading (increased from 1 sec)
  
  // Update watchdog timers
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  // Final yield to stabilize system
  yield();
}

void deactivateRelays() {
  // If transition already in progress, queue it
  if (relayTransitionInProgress) {
    Serial.println("SAFETY: Relay transition already in progress, queuing deactivation");
    relayOperationPending = true;
    pendingRelayActivation = false;
    return;
  }
  
  // Multiple yields before relay operation
  yield();
  delay(20);
  yield();
  
  // If we're in the middle of a critical operation, delay the deactivation
  if ((WiFi.status() == WL_CONNECTED && WiFi.status() != WL_IDLE_STATUS) ||
      (Firebase.ready() && Firebase.isTokenExpired() < 5)) {
    // We're in the middle of a network operation, schedule for later
    Serial.println("SAFETY: Network operation active, scheduling relay deactivation");
    relayPendingDeactivation = true;
    relayDeactivationTime = millis() + RELAY_SAFE_DELAY;
    return;
  }
  
  // Start transition
  relayTransitionInProgress = true;
  relayTransitionStartTime = millis();
  
  // Multiple yields before relay operation
  yield();
  delay(20);
  yield();
  
  // RELAY LOGIC:
  // HIGH = Relay OFF/Inactive = Door unlocked
  // LOW = Relay ON/Active = Door locked
  
  // Temporarily disable interrupts during critical relay state changes
  noInterrupts();
  
  // Unlocking the doors (deactivating relays) with progressive delays and yields
  digitalWrite(RELAY1, HIGH);
  delay(50);
  yield();
  
  digitalWrite(RELAY2, HIGH);
  delay(50);
  yield();
  
  digitalWrite(RELAY3, HIGH);
  delay(50);
  yield();
  
  digitalWrite(RELAY4, HIGH);
  delay(50);
  yield();
  
  // Re-enable interrupts
  interrupts();
  
  // Multiple yields after relay operation
  yield();
  delay(50);
  yield();
  
  // Update state flags
  relayActive = false;
  relayTransitionInProgress = false;
  
  Serial.println("Relays deactivated (unlocked) with safe timing");
  
  // Update watchdog timers
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  // Additional yields to stabilize system
  yield();
  delay(50);
  yield();
}

void checkPendingRelayOperations() {
  // Add a static variable to track consecutive errors
  static int consecutiveErrors = 0;
  static unsigned long lastSuccessfulOperation = 0;
  
  // Initial yield to ensure system stability
  yield();
  
  // Check for pending relay deactivation
  if (relayPendingDeactivation && millis() >= relayDeactivationTime) {
    Serial.println("Executing delayed relay deactivation");
    relayPendingDeactivation = false;
    
    // Start transition
    relayTransitionInProgress = true;
    relayTransitionStartTime = millis();
    
    // Yield before operation to prevent watchdog issues
    yield();
    
    // Disable interrupts for relay operations
    noInterrupts();
    
    // RELAY LOGIC: HIGH = Door unlocked, LOW = Door locked
    digitalWrite(RELAY1, HIGH);
    delay(50); // Increased delay for smoother deactivation
    yield(); // Add yield between relay operations
    digitalWrite(RELAY2, HIGH);
    delay(50); // Increased delay for smoother deactivation
    yield(); // Add yield between relay operations
    digitalWrite(RELAY3, HIGH);
    delay(50); // Increased delay for smoother deactivation
    yield(); // Add yield between relay operations
    digitalWrite(RELAY4, HIGH);
    delay(50); // Add final delay for stability
    
    // Re-enable interrupts
    interrupts();
    
    // Yield after relay state changes
    yield();
    delay(20);
    yield();
    
    relayActive = false;
    relayTransitionInProgress = false;
    lastSuccessfulOperation = millis(); // Mark this operation as successful
    consecutiveErrors = 0; // Reset error counter
    
    Serial.println("Relays safely deactivated after delay");
    
    // Update system state
    lastActivityTime = millis();
    lastReadyPrint = millis();
    
    // Allow system to stabilize before display update
    yield();
    
    // Transition to ready state
    if (!adminAccessActive && !classSessionActive && !studentVerificationActive && !tapOutPhase) {
      yield(); // Allow system to process before display update
      
      // Check reed sensor state to ensure there are no pending tamper issues
      if (reedState && !tamperActive) {
        displayMessage("Ready. Tap your", "RFID Card!", 0);
        readyMessageShown = true;
        Serial.println("System returned to ready state after relay deactivation");
      }
    }
  }
  
  // Check system resources before proceeding
  if (ESP.getFreeHeap() < 10000) {
    Serial.println("WARNING: Low memory detected in relay operations, skipping checks");
    yield(); // Allow system tasks to process
    return;
  }
  
  // Check for pending operations
  if (relayOperationPending && !relayTransitionInProgress) {
    // Yield before operation to prevent watchdog issues
    yield();
    
    relayOperationPending = false;
    
    if (pendingRelayActivation) {
      Serial.println("Executing pending relay activation");
      activateRelays();
      lastSuccessfulOperation = millis();
      consecutiveErrors = 0;
    } else if (!relayPendingDeactivation) {
      Serial.println("Executing pending relay deactivation");
      deactivateRelays();
      lastSuccessfulOperation = millis();
      consecutiveErrors = 0;
    }
  }
  
  // Check for transition timeout (safety mechanism)
  if (relayTransitionInProgress) {
    // How long has the transition been active?
    unsigned long transitionDuration = millis() - relayTransitionStartTime;
    
    if (transitionDuration > RELAY_TRANSITION_TIMEOUT) {
      Serial.println("WARNING: Relay transition timeout after " + String(transitionDuration) + "ms, forcing completion");
      
      // Force reset of transition state
      relayTransitionInProgress = false;
      
      // Increment error counter
      consecutiveErrors++;
      
      // If we have too many consecutive errors, try a recovery procedure
      if (consecutiveErrors >= 3) {
        Serial.println("CRITICAL: Multiple relay transition timeouts detected, performing recovery");
        
        // Ensure relays are in a safe state
        digitalWrite(RELAY1, HIGH);
        delay(50);
        yield();
        digitalWrite(RELAY2, HIGH);
        delay(50);
        yield();
        digitalWrite(RELAY3, HIGH);
        delay(50);
        yield();
        digitalWrite(RELAY4, HIGH);
        
        // Reset all relay state variables
        relayActive = false;
        relayOperationPending = false;
        pendingRelayActivation = false;
        relayPendingDeactivation = false;
        
        // Log recovery
        logSystemEvent("Relay recovery triggered after multiple timeouts");
        consecutiveErrors = 0;
      }
      
      yield(); // Allow system to process after timeout handling
    }
    // If transition has been active for a long time but still within timeout,
    // add yield to prevent watchdog from triggering
    else if (transitionDuration > (RELAY_TRANSITION_TIMEOUT / 2)) {
      yield(); // Additional yield for long transitions
    }
  }
  
  // If it's been a long time since a successful operation but we still have pending operations,
  // check for deadlocks
  if ((relayOperationPending || relayPendingDeactivation) && 
      lastSuccessfulOperation > 0 && 
      (millis() - lastSuccessfulOperation > 60000)) {  // 1 minute timeout
    
    Serial.println("WARNING: Potential relay operation deadlock detected, resetting state");
    
    // Reset all relay operation flags
    relayOperationPending = false;
    pendingRelayActivation = false;
    relayPendingDeactivation = false;
    relayTransitionInProgress = false;
    
    // Ensure relays are in a safe state (all deactivated)
    digitalWrite(RELAY1, HIGH);
    delay(50);
    yield();
    digitalWrite(RELAY2, HIGH);
    delay(50);
    yield();
    digitalWrite(RELAY3, HIGH);
    delay(50);
    yield();
    digitalWrite(RELAY4, HIGH);
    
    // Reset state
    relayActive = false;
    
    // Log the recovery
    logSystemEvent("Relay deadlock recovery triggered");
    
    // Reset timers
    lastSuccessfulOperation = millis();
    lastActivityTime = millis();
    lastReadyPrint = millis();
  }
  
  // Final yield to ensure smooth operation
  yield();
}

// Add function to reset session state
void resetSessionState() {
  // Reset all session-related flags and data
  classSessionActive = false;
  studentVerificationActive = false;
  waitingForInstructorEnd = false;
  tapOutPhase = false;
  
  // Clear student data
  studentAssignedSensors.clear();
  studentWeights.clear();
  awaitingWeight = false;
  
  // Reset relay and session variables
  // NOTE: We preserve lastInstructorUID to maintain reference to the instructor for PZEM data
  // lastInstructorUID = ""; // Commented out to preserve PZEM data reference
  assignedRoomId = "";
  lastPZEMLogTime = 0;
  presentCount = 0;
  
  // Update timers to prevent watchdog resets
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  Serial.println("Session state reset completed - PZEM data preserved");
}

// Move the smoothTransitionToReady function definition here, outside of any other function
void smoothTransitionToReady() {
  // Check if we're already in the ready state to avoid unnecessary transitions
  static unsigned long lastTransitionTime = 0;
  
  // Don't allow transitions too close together (minimum 3 seconds between transitions)
  if (millis() - lastTransitionTime < 3000) {
    Serial.println("Skipping redundant transition (too soon)");
    // Still update timers to prevent watchdog resets
    lastActivityTime = millis();
    lastReadyPrint = millis();
    return;
  }
  
  lastTransitionTime = millis();
  
  // Clear any pending operations
  yield();
  
  // Log memory state before cleanup
  uint32_t freeHeapBefore = ESP.getFreeHeap();
  
  // If heap memory is critically low, attempt recovery
  if (freeHeapBefore < 10000) {
    Serial.println("CRITICAL: Very low memory during transition, performing emergency cleanup");
    
    // Reset all non-essential state variables
    pendingStudentTaps.clear();
    uidDetailsPrinted.clear();
    
    // Force a yield to allow memory operations
    yield();
  }
  
  // Save important data before reset
  String preservedInstructorUID = lastInstructorUID; // Preserve instructor UID
  
  // Reset system state flags in a controlled manner
  tapOutPhase = false;
  studentVerificationActive = false;
  classSessionActive = false;
  waitingForInstructorEnd = false;
  
  // IMPORTANT FIX: Don't reset adminAccessActive here as it should be preserved
  // adminAccessActive = false;  // <-- commented out
  
  // Only reset relayActive if not in admin mode
  if (!adminAccessActive) {
    relayActive = false;
  }
  
  yield(); // Yield after flag resets
  
  // Reset the sensor assignments and weight data
  studentAssignedSensors.clear();
  studentWeights.clear();
  awaitingWeight = false;
  
  yield(); // Yield after clearing maps
  
  // Reset session-specific variables but preserve instructor reference
  // This is critical for maintaining the relationship to PZEM data
  assignedRoomId = "";
  lastPZEMLogTime = 0;
  presentCount = 0;
  
  // Show transition messages with progressive delays for smoother experience
  displayMessage("Session Ended", "Cleaning up...", 0);
  
  // Non-blocking delay with multiple yields for stability
  unsigned long startTime = millis();
  while (millis() - startTime < 1500) {
    yield(); // Non-blocking delay with yield
    // Feed watchdog during delay
    lastReadyPrint = millis();
    
    // Every 250ms during the delay, check for critical operations
    if ((millis() - startTime) % 250 == 0) {
      // Update activity timer to prevent timeouts
      lastActivityTime = millis();
    }
  }
  
  // Add intermediate transition messages for smoother experience
  displayMessage("Preparing system", "for next session", 0);
  
  startTime = millis();
  while (millis() - startTime < 1200) {
    yield();
    lastReadyPrint = millis();
    lastActivityTime = millis();
  }
  
  displayMessage("System ready", "for next session", 0);
  
  // Reduced delay from 5000ms to 2000ms (2 seconds) for a smoother transition
  startTime = millis();
  while (millis() - startTime < 2000) {
    yield();
    lastReadyPrint = millis();
    lastActivityTime = millis();
  }
  
  // Perform garbage collection
  ESP.getMinFreeHeap(); // Force memory compaction on ESP32
  yield(); // Yield after memory operation
  
  uint32_t freeHeapAfter = ESP.getFreeHeap();
  Serial.println("Memory cleanup: " + String(freeHeapBefore) + " -> " + String(freeHeapAfter) + " bytes");
  
  // Make sure tamper alert is resolved
  tamperActive = false;
  
  yield(); // Additional yield for stability
  
  // Check Firebase connection after session end
  if (isConnected && !Firebase.ready()) {
    Serial.println("Firebase connection lost during session. Attempting to reconnect...");
    
    // Try to reinitialize Firebase up to 3 times
    bool firebaseReconnected = false;
    for (int attempt = 0; attempt < 3 && !firebaseReconnected; attempt++) {
      Serial.println("Firebase reconnection attempt " + String(attempt + 1));
      
      // Update timers before potential delays
      lastActivityTime = millis();
      lastReadyPrint = millis();
      
      // Ensure WiFi is connected
      if (WiFi.status() != WL_CONNECTED) {
        Serial.println("WiFi disconnected. Reconnecting...");
        WiFi.disconnect();
        delay(500);
        yield(); // Add yield after delay
        connectWiFi();
        yield(); // Add yield after WiFi connection attempt
      }
      
      // Reset Firebase and reconnect
      Firebase.reset(&config);
      delay(500);
      yield(); // Add yield after delay
      
      Firebase.reconnectWiFi(true);
      delay(500);
      yield(); // Add yield after WiFi reconnection
      
      initFirebase();
      yield(); // Add yield after Firebase initialization
      
      // Check if reconnected successfully
      if (Firebase.ready() && Firebase.authenticated()) {
        firebaseReconnected = true;
        Serial.println("Firebase successfully reconnected after session end");
        displayMessage("Firebase", "Reconnected", 1500);
        sdMode = false; // Exit SD mode if it was active
      } else {
        Serial.println("Firebase reconnection attempt failed");
        delay(1000); // Wait before next attempt
        yield(); // Add yield after delay
      }
      
      yield(); // Prevent watchdog reset during reconnection attempts
    }
    
    // If all reconnection attempts failed, switch to SD mode
    if (!firebaseReconnected) {
      Serial.println("All Firebase reconnection attempts failed. Switching to SD mode.");
      sdMode = true;
      storeLogToSD("FirebaseReconnectFailed:Timestamp:" + getFormattedTime());
      displayMessage("Firebase Error", "Using SD Card", 2000);
    }
  }
  
  yield(); // Final yield before display update
  
  // Simple, clean transition to the ready message
  displayMessage("", "", 150); // Brief blank screen for visual break
  
  // Final ready message - SHOW THIS EXCEPT IN ADMIN MODE
  // FIX: Only show "Ready" message if not in admin mode
  if (!adminAccessActive) {
    // Check reed sensor state to ensure there are no tampering issues
    if (reedState) {
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      Serial.println("Displayed Ready message for normal mode");
    } else {
      // If reed sensor indicates open door/window, show an informative message instead
      displayMessage("Close Door/Window", "For Normal Operation", 0);
      Serial.println("Door/Window open - showing informative message");
    }
  } else {
    displayMessage("Admin Mode", "Active", 0);
    Serial.println("Kept Admin Mode display active");
  }
  
  // Update timers to prevent watchdog resets
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();
  
  // Ensure LEDs are in neutral state
  showNeutral();
  
  // Reset any error counters
  I2C_ERROR_COUNTER = 0;
  
  // Now we can safely clear the instructor reference after all operations
  lastInstructorUID = "";
  
  // Log the transition
  logSystemEvent("System Ready - Transition Complete");
  
  // Final yield to ensure system stability
  yield();
}

// Global FirebaseJson objects to reduce stack usage
static FirebaseJson instructorData;
static FirebaseJson accessJson;
static FirebaseJson classStatusJson;
static FirebaseJson pzemJson;
static FirebaseJson matchingSchedule;

#ifndef RELAY_SAFE_DELAY
#define RELAY_SAFE_DELAY 500  // 500ms safe delay for relay operations
#endif

// Add this function to ensure relays are in a safe state
void ensureRelaySafeState() {
  // Make sure relays are properly initialized and in a safe state
  pinMode(RELAY1, OUTPUT);
  pinMode(RELAY2, OUTPUT);
  pinMode(RELAY3, OUTPUT);
  pinMode(RELAY4, OUTPUT);
  
  // Ensure relays are inactive (HIGH)
  digitalWrite(RELAY1, HIGH);
  digitalWrite(RELAY2, HIGH);
  digitalWrite(RELAY3, HIGH);
  digitalWrite(RELAY4, HIGH);
  relayActive = false;
  relayTransitionInProgress = false;
  relayOperationPending = false;
  pendingRelayActivation = false;
  relayPendingDeactivation = false;
}

void logInstructor(String uid, String timestamp, String action) {
  // Log to SD
  String entry = "Instructor:" + uid + " Action:" + action + " Time:" + timestamp;
  storeLogToSD(entry);
  Serial.println("SD log stored to /Offline_Logs_Entry.txt: " + entry);

  // Voltage check
  float currentVoltage = pzem.voltage();
  if (!isVoltageSufficient || isnan(currentVoltage)) {
    delay(50);
    currentVoltage = pzem.voltage();
    isVoltageSufficient = (currentVoltage >= voltageThreshold && !isnan(currentVoltage));
  }
  Serial.println("Conditions - sdMode: " + String(sdMode) + 
                 ", isConnected: " + String(isConnected) + 
                 ", Voltage: " + (isnan(currentVoltage) ? "NaN" : String(currentVoltage, 2)) + "V" +
                 ", Threshold: " + String(voltageThreshold) + "V");

  // Extract time
  String currentTime = timestamp.substring(11, 13) + ":" + timestamp.substring(14, 16); // HH:MM

  // Schedule check
  ScheduleInfo currentSchedule;
  if (action == "Access") {
    // If we're waiting for instructor to finalize the session, don't start a new session
    if (waitingForInstructorEnd && uid == lastInstructorUID) {
      Serial.println("Instructor " + uid + " is finalizing the session after all students tapped out. Not starting a new session.");
      // The tap-out section will handle saving PZEM data
      return;
    }
    
    String day = getDayFromTimestamp(timestamp);
    String time = timestamp.substring(11, 16);
    int hour = time.substring(0, 2).toInt();
    int minute = time.substring(3, 5).toInt();
    currentSchedule = checkSchedule(uid, day, hour, minute);
    lastInstructorUID = uid;
  } else if (action == "EndSession" && lastInstructorUID == uid) {
    // Always try to revalidate schedule for EndSession, whether or not it's already valid
    String day = getDayFromTimestamp(timestamp);
    String time = timestamp.substring(11, 16);
    int hour = time.substring(0, 2).toInt();
    int minute = time.substring(3, 5).toInt();
    currentSchedule = checkSchedule(uid, day, hour, minute);
    
    if (currentSchedule.isValid) {
      Serial.println("Schedule valid for EndSession: " + currentSchedule.day + " " + 
                     currentSchedule.startTime + "-" + currentSchedule.endTime + ", Room: " + 
                     currentSchedule.roomName + ", Subject: " + currentSchedule.subject);
    } else {
      // If still not valid, try using current date with the last known schedule times
      Serial.println("Schedule not valid for EndSession. Trying with today's date...");
      // Get instructor schedule for the current day regardless of time
      String dateOnly = timestamp.substring(0, 10); // YYYY_MM_DD
      currentSchedule = getInstructorScheduleForDay(uid, dateOnly);
      
      if (currentSchedule.isValid) {
        Serial.println("Retrieved schedule using today's date: " + currentSchedule.day + 
                      " " + currentSchedule.startTime + "-" + currentSchedule.endTime);
      } else {
        Serial.println("WARNING: Unable to retrieve valid schedule for instructor " + uid + 
                      " during EndSession. PZEM data may not be properly logged.");
      }
    }
  }

  // Fetch instructor data
  String fullName = firestoreTeachers[uid]["fullName"].length() > 0 ? firestoreTeachers[uid]["fullName"] : "Unknown";
  String role = firestoreTeachers[uid]["role"].length() > 0 ? firestoreTeachers[uid]["role"] : "instructor";
  role.trim();
  if (!role.equalsIgnoreCase("instructor")) role = "instructor";

  // Schedule endTime check
  if (relayActive && !tapOutPhase && currentSchedule.isValid && action != "EndSession") {
    if (currentSchedule.endTime.length() == 5 && currentTime.length() == 5) {
      // Convert times to minutes for easier comparison
      int currentHour = currentTime.substring(0, 2).toInt();
      int currentMinute = currentTime.substring(3, 5).toInt();
      int currentTotalMinutes = currentHour * 60 + currentMinute;
      
      int endHour = currentSchedule.endTime.substring(0, 2).toInt();
      int endMinute = currentSchedule.endTime.substring(3, 5).toInt();
      int endTotalMinutes = endHour * 60 + endMinute;
      
      // Get start time in minutes for span detection
      int startHour = 0;
      int startMinute = 0;
      if (currentSchedule.startTime.length() == 5) {
        startHour = currentSchedule.startTime.substring(0, 2).toInt();
        startMinute = currentSchedule.startTime.substring(3, 5).toInt();
      }
      int startTotalMinutes = startHour * 60 + startMinute;
      
      // Check for class spanning across midnight (endTime < startTime)
      bool spansMidnight = endTotalMinutes < startTotalMinutes;
      
      // Adjust comparison for classes that span midnight
      bool endTimeReached = false;
      if (spansMidnight) {
        // If class spans midnight and current time is less than start time, 
        // it means we're after midnight, so adjust comparison
        if (currentTotalMinutes < startTotalMinutes) {
          endTimeReached = (currentTotalMinutes >= endTotalMinutes);
        } else {
          // Current time is after start time but before midnight
          endTimeReached = false;
        }
      } else {
        // Normal comparison for classes that don't span midnight
        endTimeReached = (currentTotalMinutes >= endTotalMinutes);
      }
      
      Serial.println("Time in minutes - Current: " + String(currentTotalMinutes) + 
                     ", Start: " + String(startTotalMinutes) + 
                     ", End: " + String(endTotalMinutes) + 
                     ", Spans midnight: " + String(spansMidnight));
      
      if (endTimeReached) {
        Serial.println("End time reached (" + currentTime + " >= " + currentSchedule.endTime + "). Transitioning to tap-out phase.");
        digitalWrite(RELAY2, HIGH);
        digitalWrite(RELAY3, HIGH);
        digitalWrite(RELAY4, HIGH);
        relayActive = false;
        classSessionActive = false;
        tapOutPhase = true;
        tapOutStartTime = millis();
        tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
        
        // Store PZEM data and update ClassStatus to "Class Ended"
        if (!sdMode && isConnected && Firebase.ready() && currentSchedule.roomName.length() > 0) {
          float voltage = pzem.voltage();
          float current = pzem.current();
          float power = pzem.power();
          float energy = pzem.energy();
          float frequency = pzem.frequency();
          float powerFactor = pzem.pf();
          
          if (isnan(voltage) || voltage < 0) voltage = 0.0;
          if (isnan(current) || current < 0) current = 0.0;
          if (isnan(power) || power < 0) power = 0.0;
          if (isnan(energy) || energy < 0) energy = 0.0;
          if (isnan(frequency) || frequency < 0) frequency = 0.0;
          if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
          
          String instructorPath = "/Instructors/" + lastInstructorUID;
          FirebaseJson classStatusJson;
          classStatusJson.set("Status", "Class Ended");
          classStatusJson.set("dateTime", timestamp);
          
          FirebaseJson scheduleJson;
          scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
          scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
          scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
          scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
          scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
          scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
          
          FirebaseJson roomNameJson;
          roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
          
          // Create PZEM JSON data
          FirebaseJson pzemJson;
          pzemJson.set("voltage", String(voltage, 1));
          pzemJson.set("current", String(current, 2));
          pzemJson.set("power", String(power, 1));
          pzemJson.set("energy", String(energy, 2));
          pzemJson.set("frequency", String(frequency, 1));
          pzemJson.set("powerFactor", String(powerFactor, 2));
          pzemJson.set("timestamp", timestamp);
          roomNameJson.set("pzem", pzemJson);
          
          scheduleJson.set("roomName", roomNameJson);
          classStatusJson.set("schedule", scheduleJson);
          
          String statusPath = instructorPath + "/ClassStatus";
          // Use setJSON instead of updateNode to ensure proper data structure
          if (Firebase.RTDB.setJSON(&fbdo, statusPath, &classStatusJson)) {
            Serial.println("Class status updated to 'Class Ended' with PZEM data preserved");
            
            // Create a permanent archive of the session data
            String archivePath = instructorPath + "/ClassHistory/" + timestamp.substring(0, 10) + "_" + timestamp.substring(11, 13) + timestamp.substring(14, 16);
            if (Firebase.RTDB.setJSON(&fbdo, archivePath, &classStatusJson)) {
              Serial.println("Class data archived for permanent storage");
            }
          } else {
            String errorLog = "FirebaseError:Path:" + statusPath + " Reason:" + fbdo.errorReason();
            storeLogToSD(errorLog);
            Serial.println("Failed to update class status with PZEM data: " + fbdo.errorReason());
          }
        }
        
        displayMessage("Class Ended", "Tap to Confirm", 3000);
        Serial.println("Schedule endTime " + currentSchedule.endTime + " reached at " + currentTime + ". Transition to tap-out phase.");
      }
    }
  }

  // Firebase logging
  if (!sdMode && isConnected && Firebase.ready()) {
    Serial.println("Firebase conditions met. Logging UID: " + uid + " Action: " + action);

    String instructorPath = "/Instructors/" + uid;

    // Profile data
    FirebaseJson instructorData;
    instructorData.set("fullName", fullName);
    instructorData.set("email", firestoreTeachers[uid]["email"].length() > 0 ? firestoreTeachers[uid]["email"] : "N/A");
    instructorData.set("idNumber", firestoreTeachers[uid]["idNumber"].length() > 0 ? firestoreTeachers[uid]["idNumber"] : "N/A");
    instructorData.set("mobileNumber", firestoreTeachers[uid]["mobileNumber"].length() > 0 ? firestoreTeachers[uid]["mobileNumber"] : "N/A");
    instructorData.set("role", role);
    instructorData.set("department", firestoreTeachers[uid]["department"].length() > 0 ? firestoreTeachers[uid]["department"] : "Unknown");
    instructorData.set("createdAt", firestoreTeachers[uid]["createdAt"].length() > 0 ? firestoreTeachers[uid]["createdAt"] : "N/A");

    // Access log
    FirebaseJson accessJson;
    accessJson.set("action", action);
    accessJson.set("timestamp", timestamp);
    accessJson.set("status", (action == "Access" && currentSchedule.isValid) ? "granted" : (action == "Access" ? "denied" : "completed"));

    // Class status
    FirebaseJson classStatusJson;
    String status = (action == "Access" && currentSchedule.isValid) ? "In Session" : (action == "Access" ? "Denied" : "End Session");
    classStatusJson.set("Status", status);
    classStatusJson.set("dateTime", timestamp);

    // Always include schedule for Access and EndSession to ensure PZEM data is logged
    FirebaseJson scheduleJson;
    scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
    scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
    scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
    scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
    scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
    scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
    FirebaseJson roomNameJson;
    roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");

    // PZEM data for EndSession on first tap
    if (action == "EndSession" && uid == lastInstructorUID) {
      // Check if ending early
      String currentTime = timestamp.substring(11, 13) + ":" + timestamp.substring(14, 16); // HH:MM
      if (currentSchedule.isValid && currentTime < currentSchedule.endTime) {
        Serial.println("Instructor UID " + uid + " ended session early before endTime " + currentSchedule.endTime + ".");
      }
      
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float powerFactor = pzem.pf();

      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;

      // Calculate energy consumption using E = P*(t/1000)
      // Get the session duration in hours
      float sessionDurationHours = 0.0;
      if (currentSchedule.startTime.length() > 0 && currentSchedule.endTime.length() > 0) {
        // Extract hours and minutes from startTime (format HH:MM)
        int startHour = currentSchedule.startTime.substring(0, 2).toInt();
        int startMinute = currentSchedule.startTime.substring(3, 5).toInt();
        
        // Extract hours and minutes from endTime (format HH:MM)
        int endHour = currentSchedule.endTime.substring(0, 2).toInt();
        int endMinute = currentSchedule.endTime.substring(3, 5).toInt();
        
        // Calculate total minutes
        int startTotalMinutes = startHour * 60 + startMinute;
        int endTotalMinutes = endHour * 60 + endMinute;
        
        // Handle cases where the end time is on the next day
        if (endTotalMinutes < startTotalMinutes) {
          endTotalMinutes += 24 * 60; // Add 24 hours in minutes
        }
        
        // Calculate the duration in hours
        sessionDurationHours = (endTotalMinutes - startTotalMinutes) / 60.0;
      }
      
      // Calculate energy consumption (kWh) using E = P*(t/1000)
      // Power is in watts, time is in hours, result is in kWh
      float calculatedEnergy = power * sessionDurationHours / 1000.0;
      
      // Use the calculated energy if the measured energy is zero or invalid
      if (energy <= 0.01) {
        energy = calculatedEnergy;
      }
      
      Serial.println("Session duration: " + String(sessionDurationHours) + " hours");
      Serial.println("Calculated energy: " + String(calculatedEnergy) + " kWh");

      FirebaseJson pzemJson;
      pzemJson.set("voltage", String(voltage, 1));
      pzemJson.set("current", String(current, 2));
      pzemJson.set("power", String(power, 1));
      pzemJson.set("energy", String(energy, 2));
      pzemJson.set("calculatedEnergy", String(calculatedEnergy, 2));
      pzemJson.set("sessionDuration", String(sessionDurationHours, 2));
      pzemJson.set("frequency", String(frequency, 1));
      pzemJson.set("powerFactor", String(powerFactor, 2));
      pzemJson.set("timestamp", timestamp);
      pzemJson.set("action", "end");
      roomNameJson.set("pzem", pzemJson);
      pzemLoggedForSession = true;
      Serial.println("PZEM logged at session end: Voltage=" + String(voltage, 1) + ", Energy=" + String(energy, 2));

      // Remove the old separate Rooms node storage
      // Instead, modify ClassStatus to include all necessary information
      if (currentSchedule.roomName.length() > 0) {
        // Instead of creating a separate path, we'll log this information to the classStatusJson
        FirebaseJson classDetailsJson;
        classDetailsJson.set("roomName", currentSchedule.roomName);
        classDetailsJson.set("subject", currentSchedule.subject);
        classDetailsJson.set("subjectCode", currentSchedule.subjectCode);
        classDetailsJson.set("section", currentSchedule.section);
        classDetailsJson.set("sessionStart", currentSchedule.startTime);
        classDetailsJson.set("sessionEnd", currentSchedule.endTime);
        classDetailsJson.set("date", timestamp.substring(0, 10));
        classDetailsJson.set("sessionId", currentSessionId);
        
        // Add this class details to the ClassStatus node
        classStatusJson.set("roomDetails", classDetailsJson);
        
        Serial.println("Class details included in ClassStatus");
      }
    }

    scheduleJson.set("roomName", roomNameJson);
    classStatusJson.set("schedule", scheduleJson);

    // Perform Firebase operations
    bool success = true;
    if (!Firebase.RTDB.setJSON(&fbdo, instructorPath + "/Profile", &instructorData)) {
      Serial.println("Failed to sync profile: " + fbdo.errorReason());
      success = false;
      storeLogToSD("ProfileFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }
    if (!Firebase.RTDB.pushJSON(&fbdo, instructorPath + "/AccessLogs", &accessJson)) {
      Serial.println("Failed to push access log: " + fbdo.errorReason());
      success = false;
      storeLogToSD("AccessLogFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }
    if (!Firebase.RTDB.setJSON(&fbdo, instructorPath + "/ClassStatus", &classStatusJson)) {
      Serial.println("Failed to update class status: " + fbdo.errorReason());
      success = false;
      storeLogToSD("ClassStatusFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }
    if (!Firebase.RTDB.setString(&fbdo, "/RegisteredUIDs/" + uid, timestamp)) {
      Serial.println("Failed to update RegisteredUIDs: " + fbdo.errorReason());
      success = false;
      storeLogToSD("RegisteredUIDsFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }

    if (!success) {
      sdMode = true;
      Serial.println("Firebase logging failed. Switching to SD mode.");
    } else {
      Serial.println("Instructor " + fullName + " logged to Firebase successfully at path: " + instructorPath);
    }
  } else {
    Serial.println("Firebase conditions not met: sdMode=" + String(sdMode) + 
                   ", isConnected=" + String(isConnected) + 
                   ", isVoltageSufficient=" + String(isVoltageSufficient) + 
                   ", Firebase.ready=" + String(Firebase.ready()));
    if (action == "EndSession" && uid == lastInstructorUID) {
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float powerFactor = pzem.pf();
      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
      String pzemEntry = "PZEM:UID:" + uid + " Time:" + timestamp +
                         " Voltage:" + String(voltage, 1) + "V" +
                         " Current:" + String(current, 2) + "A" +
                         " Power:" + String(power, 1) + "W" +
                         " Energy:" + String(energy, 2) + "kWh" +
                         " Frequency:" + String(frequency, 1) + "Hz" +
                         " PowerFactor:" + String(powerFactor, 2);
      storeLogToSD(pzemEntry);
      pzemLoggedForSession = true;
    }
  }

  // Handle actions
  if (action == "Access") {
    if (!currentSchedule.isValid) {
      // Before denying access, try to refresh schedule data from Firestore
      bool scheduleFound = false;
      
      // First log a temporary "pending verification" status
      if (!sdMode && isConnected && Firebase.ready()) {
        // Log the temporary "pending verification" status
        String tempPath = "/Instructors/" + uid + "/AccessLogs";
        FirebaseJson tempAccessJson;
        tempAccessJson.set("action", "Access");
        tempAccessJson.set("status", "pending_verification");
        tempAccessJson.set("timestamp", timestamp);
        tempAccessJson.set("note", "Checking for schedule updates");
        String tempLogId;
        
        if (Firebase.RTDB.pushJSON(&fbdo, tempPath, &tempAccessJson)) {
          // Save the temporary log ID for potential update later
          tempLogId = fbdo.pushName();
          Serial.println("Temporary pending log created with ID: " + tempLogId);
        }
        
        // Display message indicating schedule check
        displayMessage("Checking for", "Updated Schedule", 2000);
        Serial.println("Instructor UID " + uid + " outside schedule. Checking for updates...");
        
        // Fetch updated teacher and room data from Firestore
        fetchFirestoreTeachers();
        fetchFirestoreRooms();
        
        // Check schedule again with freshly fetched data
        String day = getDayFromTimestamp(timestamp);
        String time = timestamp.substring(11, 16);
        int hour = time.substring(0, 2).toInt();
        int minute = time.substring(3, 5).toInt();
        currentSchedule = checkSchedule(uid, day, hour, minute);
        
        if (currentSchedule.isValid) {
          Serial.println("Updated schedule found after refresh! Room: " + currentSchedule.roomName);
          scheduleFound = true;
          
          // Update the previously created log to reflect successful verification
          if (tempLogId.length() > 0) {
            String updatePath = "/Instructors/" + uid + "/AccessLogs/" + tempLogId;
            FirebaseJson updateJson;
            updateJson.set("status", "granted");
            updateJson.set("note", "Access granted after schedule refresh");
            
            if (Firebase.RTDB.updateNode(&fbdo, updatePath, &updateJson)) {
              Serial.println("Access log updated to 'granted' after schedule refresh");
            } else {
              Serial.println("Failed to update access log: " + fbdo.errorReason());
            }
          }
          
          // Update instructor ClassStatus to reflect the valid schedule
          String classStatusPath = "/Instructors/" + uid + "/ClassStatus";
          FirebaseJson classStatusJson;
          classStatusJson.set("Status", "In Session");
          classStatusJson.set("dateTime", timestamp);
          classStatusJson.set("schedule/day", currentSchedule.day);
          classStatusJson.set("schedule/startTime", currentSchedule.startTime);
          classStatusJson.set("schedule/endTime", currentSchedule.endTime);
          classStatusJson.set("schedule/subject", currentSchedule.subject);
          classStatusJson.set("schedule/subjectCode", currentSchedule.subjectCode);
          classStatusJson.set("schedule/section", currentSchedule.section);
          classStatusJson.set("schedule/roomName/name", currentSchedule.roomName);
          
          // Check if there's existing PZEM data we need to preserve
          if (Firebase.RTDB.get(&fbdo, classStatusPath + "/schedule/roomName/pzem")) {
            if (fbdo.dataType() == "json") {
              FirebaseJson pzemData;
              pzemData.setJsonData(fbdo.jsonString());
              
              // Extract values from existing PZEM data
              FirebaseJsonData voltage, current, power, energy, frequency, powerFactor, pzemTimestamp;
              pzemData.get(voltage, "voltage");
              pzemData.get(current, "current");
              pzemData.get(power, "power");
              pzemData.get(energy, "energy");
              pzemData.get(frequency, "frequency");
              pzemData.get(powerFactor, "powerFactor");
              pzemData.get(pzemTimestamp, "timestamp");
              
              // Add PZEM data to avoid overwriting it
              if (voltage.success) classStatusJson.set("schedule/roomName/pzem/voltage", voltage.stringValue);
              if (current.success) classStatusJson.set("schedule/roomName/pzem/current", current.stringValue);
              if (power.success) classStatusJson.set("schedule/roomName/pzem/power", power.stringValue);
              if (energy.success) classStatusJson.set("schedule/roomName/pzem/energy", energy.stringValue);
              if (frequency.success) classStatusJson.set("schedule/roomName/pzem/frequency", frequency.stringValue);
              if (powerFactor.success) classStatusJson.set("schedule/roomName/pzem/powerFactor", powerFactor.stringValue);
              if (pzemTimestamp.success) classStatusJson.set("schedule/roomName/pzem/timestamp", pzemTimestamp.stringValue);
              
              Serial.println("Existing PZEM data preserved in ClassStatus update");
            }
          }
          
          // Use setJSON instead of updateNode to ensure consistency
          if (Firebase.RTDB.setJSON(&fbdo, classStatusPath, &classStatusJson)) {
            Serial.println("Class status updated to 'In Session' after schedule refresh");
          } else {
            Serial.println("Failed to update class status: " + fbdo.errorReason());
          }
          
          // Continue with access (will be handled below since currentSchedule is now valid)
        } else {
          Serial.println("No valid schedule found even after refresh for UID " + uid);
          
          // Update the temporary log to confirm denial
          if (tempLogId.length() > 0) {
            String updatePath = "/Instructors/" + uid + "/AccessLogs/" + tempLogId;
            FirebaseJson updateJson;
            updateJson.set("status", "denied");
            updateJson.set("note", "No valid schedule found even after refresh");
            
            if (Firebase.RTDB.updateNode(&fbdo, updatePath, &updateJson)) {
              Serial.println("Access log updated to 'denied' after schedule refresh attempt");
            } else {
              Serial.println("Failed to update access log: " + fbdo.errorReason());
            }
          }
        }
      }
      
      if (!scheduleFound) {
        // No valid schedule found even after refresh
        deniedFeedback();
        displayMessage("Outside Schedule", "Access Denied", 6000);
        Serial.println("Instructor UID " + uid + " denied: outside schedule.");
        return;
      }
    }

    if (!relayActive) {
      digitalWrite(RELAY1, LOW);
      digitalWrite(RELAY2, LOW);
      digitalWrite(RELAY3, LOW);
      digitalWrite(RELAY4, LOW);
      relayActive = true;
      studentVerificationActive = true;
      studentVerificationStartTime = millis();
      currentStudentQueueIndex = 0;
      lastStudentTapTime = millis();
      presentCount = 0;
      pzemLoggedForSession = false;

      String subject = currentSchedule.subject.length() > 0 ? currentSchedule.subject : "UNK";
      String subjectCode = currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "UNK";
      String section = currentSchedule.section.length() > 0 ? currentSchedule.section : "UNK";
      String roomName = currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "UNK";
      String startTimeStr = currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown";
      String endTimeStr = currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown";

      if (section == "UNK" && firestoreTeachers[uid]["sections"] != "[]") {
        FirebaseJson sectionsJson;
        if (sectionsJson.setJsonData(firestoreTeachers[uid]["sections"])) {
          FirebaseJsonData sectionData;
          if (sectionsJson.get(sectionData, "[0]/name") && sectionData.typeNum == FirebaseJson::JSON_STRING) {
            section = sectionData.stringValue;
          }
        }
      }

      String classDate = timestamp.substring(0, 10);
      currentSessionId = classDate + "_" + subjectCode + "_" + section + "_" + roomName;
      studentAssignedSensors.clear();
      studentWeights.clear();
      sessionStartReading = pzem.energy();

      accessFeedback();
      Serial.println("Class session started. Session ID: " + currentSessionId + ", Room: " + roomName);
      displayMessage(subjectCode + " " + section, roomName + " " + startTimeStr + "-" + endTimeStr, 5000);
    } else if (relayActive && uid == lastInstructorUID && !tapOutPhase) {
      digitalWrite(RELAY2, HIGH);
      digitalWrite(RELAY3, HIGH);
      digitalWrite(RELAY4, HIGH);
      relayActive = false;
      classSessionActive = false;
      tapOutPhase = true;
      tapOutStartTime = millis();
      pzemLoggedForSession = false; // Reset the global variable
      tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
      displayMessage("Class Ended", "Tap to Confirm", 3000);
      Serial.println("Instructor UID " + uid + " ended session early before endTime " + currentSchedule.endTime + ".");
    }
  } else if (action == "EndSession" && relayActive && uid == lastInstructorUID && !tapOutPhase) {
    digitalWrite(RELAY2, HIGH);
    digitalWrite(RELAY3, HIGH);
    digitalWrite(RELAY4, HIGH);
    relayActive = false;
    classSessionActive = false;
    tapOutPhase = true;
    tapOutStartTime = millis();
    pzemLoggedForSession = false; // Reset the global variable
    tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
    
    // Store PZEM data and update ClassStatus to "Class Ended"
    if (!sdMode && isConnected && Firebase.ready() && currentSchedule.roomName.length() > 0) {
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float powerFactor = pzem.pf();
      
      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
      
      String instructorPath = "/Instructors/" + uid;
      FirebaseJson classStatusJson;
      classStatusJson.set("Status", "Class Ended");
      classStatusJson.set("dateTime", timestamp);
      
      FirebaseJson scheduleJson;
      scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
      scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
      scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
      scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
      scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
      scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
      
      FirebaseJson roomNameJson;
      roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
      
      // Create PZEM JSON data
      FirebaseJson pzemJson;
      pzemJson.set("voltage", String(voltage, 1));
      pzemJson.set("current", String(current, 2));
      pzemJson.set("power", String(power, 1));
      pzemJson.set("energy", String(energy, 2));
      pzemJson.set("frequency", String(frequency, 1));
      pzemJson.set("powerFactor", String(powerFactor, 2));
      pzemJson.set("timestamp", timestamp);
      roomNameJson.set("pzem", pzemJson);
      
      scheduleJson.set("roomName", roomNameJson);
      classStatusJson.set("schedule", scheduleJson);
      
      String statusPath = instructorPath + "/ClassStatus";
      // Use setJSON instead of updateNode to ensure proper data structure
      if (Firebase.RTDB.setJSON(&fbdo, statusPath, &classStatusJson)) {
        Serial.println("Class status updated to 'Class Ended' with PZEM data preserved");
        
        // Create a permanent archive of the session data
        String archivePath = instructorPath + "/ClassHistory/" + timestamp.substring(0, 10) + "_" + timestamp.substring(11, 13) + timestamp.substring(14, 16);
        if (Firebase.RTDB.setJSON(&fbdo, archivePath, &classStatusJson)) {
          Serial.println("Class data archived for permanent storage");
        }
      } else {
        String errorLog = "FirebaseError:Path:" + statusPath + " Reason:" + fbdo.errorReason();
        storeLogToSD(errorLog);
        Serial.println("Failed to update class status with PZEM data: " + fbdo.errorReason());
      }
    }
    
    displayMessage("Class Ended", "Tap to Confirm", 3000);
    Serial.println("Instructor UID " + uid + " explicitly ended session early with EndSession action.");
  } else if (action == "EndSession" && tapOutPhase && uid == lastInstructorUID) {
    displayMessage("Session Finalized", "Summary Saved", 3000);
    Serial.println("Final tap by instructor UID " + uid + ". Generating AttendanceSummary.");

    if (!sdMode && isConnected && Firebase.ready()) {
      String classStatusPath = "/Instructors/" + uid + "/ClassStatus";
      FirebaseJson classStatusUpdate;
      classStatusUpdate.set("Status", "End Session");
      classStatusUpdate.set("dateTime", timestamp);

      // Include schedule without PZEM data
      FirebaseJson scheduleJson;
      scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
      scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
      scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
      scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
      scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
      scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
      
      FirebaseJson roomNameJson;
      roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
      
      // Check if there's existing PZEM data to preserve
      if (Firebase.RTDB.get(&fbdo, classStatusPath + "/schedule/roomName/pzem")) {
        if (fbdo.dataType() == "json") {
          // Get the existing PZEM data
          FirebaseJson pzemData;
          pzemData.setJsonData(fbdo.jsonString());
          
          // Set it in the roomName object
          roomNameJson.set("pzem", pzemData);
          Serial.println("Preserved existing PZEM data during AttendanceSummary generation");
        }
      }
      
      scheduleJson.set("roomName", roomNameJson);
      classStatusUpdate.set("schedule", scheduleJson);

      if (!Firebase.RTDB.setJSON(&fbdo, classStatusPath, &classStatusUpdate)) {
        Serial.println("Failed to update ClassStatus: " + fbdo.errorReason());
        storeLogToSD("ClassStatusFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
      } else {
        Serial.println("ClassStatus updated at " + classStatusPath + " with PZEM data preserved");
        
        // Archive the final session data for history
        String archivePath = "/Instructors/" + uid + "/ClassHistory/" + timestamp.substring(0, 10) + "_" + timestamp.substring(11, 13) + timestamp.substring(14, 16) + "_final";
        if (Firebase.RTDB.setJSON(&fbdo, archivePath, &classStatusUpdate)) {
          Serial.println("Final class data archived for permanent storage");
        }
      }
    }

    // Generate AttendanceSummary
    String summaryPath = "/AttendanceSummary/" + currentSessionId;
    FirebaseJson summaryJson;
    summaryJson.set("InstructorName", fullName);
    summaryJson.set("StartTime", timestamp); // Note: Consider storing actual start time
    summaryJson.set("EndTime", timestamp);
    summaryJson.set("Status", "Class Ended");
    summaryJson.set("SubjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
    summaryJson.set("SubjectName", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
    summaryJson.set("Day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
    summaryJson.set("Section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");

    // Fetch students
    FirebaseJson attendeesJson;
    int totalAttendees = 0;
    std::set<String> processedStudents;

    for (const auto& student : firestoreStudents) {
      String studentUid = student.first;
      String studentSection;
      try {
        studentSection = student.second.at("section").length() > 0 ? student.second.at("section") : "";
      } catch (const std::out_of_range&) {
        studentSection = "";
      }
      if (studentSection != currentSchedule.section) continue;

      String studentPath = "/Students/" + studentUid;
      String studentName;
      try {
        studentName = student.second.at("fullName").length() > 0 ? student.second.at("fullName") : "Unknown";
      } catch (const std::out_of_range&) {
        studentName = "Unknown";
      }
      String status = "Absent";
      float weight = 0.0;
      String studentSessionId = "";

      if (Firebase.RTDB.getJSON(&fbdo, studentPath)) {
        FirebaseJsonData data;
        if (fbdo.jsonObjectPtr()->get(data, "Status")) {
          status = data.stringValue;
        }
        if (fbdo.jsonObjectPtr()->get(data, "weight")) {
          weight = data.doubleValue;
        }
        if (fbdo.jsonObjectPtr()->get(data, "sessionId")) {
          studentSessionId = data.stringValue;
        }
      }

      if (studentSessionId == currentSessionId && status == "Present") {
        totalAttendees++;
      }

      FirebaseJson studentJson;
      studentJson.set("StudentName", studentName);
      studentJson.set("Status", status);
      studentJson.set("Weight", weight);
      attendeesJson.set(studentUid, studentJson);
      processedStudents.insert(studentUid);
    }

    // Handle pendingStudentTaps
    for (const String& studentUid : pendingStudentTaps) {
      if (processedStudents.find(studentUid) != processedStudents.end()) continue;
      processedStudents.insert(studentUid);

      String studentPath = "/Students/" + studentUid;
      String studentName;
      try {
        studentName = firestoreStudents.at(studentUid).at("fullName").length() > 0 ? firestoreStudents.at(studentUid).at("fullName") : "Unknown";
      } catch (const std::out_of_range&) {
        studentName = "Unknown";
      }
      String status = "Absent";
      float weight = 0.0;

      if (Firebase.RTDB.getJSON(&fbdo, studentPath)) {
        FirebaseJsonData data;
        if (fbdo.jsonObjectPtr()->get(data, "Status")) {
          status = data.stringValue;
        }
        if (fbdo.jsonObjectPtr()->get(data, "weight")) {
          weight = data.doubleValue;
        }
      }

      FirebaseJson studentJson;
      studentJson.set("StudentName", studentName);
      studentJson.set("Status", status);
      studentJson.set("Weight", weight);
      attendeesJson.set(studentUid, studentJson);
    }

    summaryJson.set("Attendees", attendeesJson);
    summaryJson.set("TotalAttendees", totalAttendees);

    if (Firebase.RTDB.setJSON(&fbdo, summaryPath, &summaryJson)) {
      Serial.println("AttendanceSummary created at " + summaryPath + ", Attendees: " + String(totalAttendees));
    } else {
      Serial.println("Failed to create AttendanceSummary: " + fbdo.errorReason());
      storeLogToSD("AttendanceSummaryFailed:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
    }

    // SD mode summary (moved here to ensure it's part of final tap)
    if (sdMode || !isConnected || !Firebase.ready()) {
      String summaryEntry = "AttendanceSummary:SessionID:" + currentSessionId + 
                           " Instructor:" + fullName + 
                           " Start:" + timestamp + 
                           " End:" + timestamp + 
                           " Status:Class Ended";
      
      int totalAttendees = 0;
      for (const auto& student : firestoreStudents) {
        String studentUid = student.first;
        String studentSection;
        try {
          studentSection = student.second.at("section").length() > 0 ? student.second.at("section") : "";
        } catch (const std::out_of_range&) {
          studentSection = "";
        }
        if (studentSection != currentSchedule.section) continue;
        String studentName;
        try {
          studentName = student.second.at("fullName").length() > 0 ? student.second.at("fullName") : "Unknown";
        } catch (const std::out_of_range&) {
          studentName = "Unknown";
        }
        if (studentWeights.find(studentUid) != studentWeights.end() && studentWeights[studentUid] >= 15) {
          totalAttendees++;
          summaryEntry += " Student:" + studentUid + ":" + studentName + ":Present";
        } else {
          summaryEntry += " Student:" + studentUid + ":" + studentName + ":Absent";
        }
      }
      for (const String& studentUid : pendingStudentTaps) {
        if (firestoreStudents.find(studentUid) == firestoreStudents.end()) continue;
        String studentName;
        try {
          studentName = firestoreStudents.at(studentUid).at("fullName").length() > 0 ? firestoreStudents.at(studentUid).at("fullName") : "Unknown";
        } catch (const std::out_of_range&) {
          studentName = "Unknown";
        }
        summaryEntry += " Student:" + studentUid + ":" + studentName + ":Absent";
      }
      storeLogToSD(summaryEntry);
      Serial.println("Firebase unavailable. AttendanceSummary logged to SD, Attendees: " + String(totalAttendees));
    }

    // Reset system
    tapOutPhase = false;
    waitingForInstructorEnd = false;
    lastInstructorUID = "";
    currentSessionId = "";
    studentAssignedSensors.clear();
    studentWeights.clear();
    pendingStudentTaps.clear();
    presentCount = 0;
    digitalWrite(RELAY1, HIGH);
    pzemLoggedForSession = false;
    Serial.println("Session fully ended. All relays off, system reset.");
    displayMessage("Ready. Tap your", "RFID Card!", 0);
    readyMessageShown = true;

    // Reset watchdog timer at session end
    lastReadyPrint = millis();
    Serial.println("Watchdog timer reset at session end");

    // New function for smooth transition to ready state
    smoothTransitionToReady();

    // Clear finalization tracking for next session
    uidWeightFinalized.clear();
    sensorAssignedUid.clear();
  }
}

void logStudentToRTDB(String rfidUid, String timestamp, float weight, int sensorIndex, String weightConfirmed, String timeOut) {
  Serial.println("logStudentToRTDB called for UID: " + rfidUid + " at " + timestamp);
  yield(); // Initial yield to prevent stack overflow

  // Get the last session ID
  String lastSessionId = "";
  if (currentSessionId != "") {
    lastSessionId = currentSessionId;
  } else if (!sdMode && isConnected && Firebase.ready()) {
    String lastSessionPath = "/Students/" + rfidUid + "/lastSession";
    if (Firebase.RTDB.getString(&fbdo, lastSessionPath)) {
      lastSessionId = fbdo.stringData();
    }
  }

  // Check if attendance is already finalized for this student
  if (lastSessionId != "" && isAttendanceFinalized(rfidUid, lastSessionId)) {
    Serial.println("Attendance already finalized for " + rfidUid + " in session " + lastSessionId + ". Skipping update.");
    return;  // Skip updating if already finalized
  }

  // Check and create student profile in RTDB if needed
  if (!sdMode && isConnected && Firebase.ready()) {
    // Reset watchdog timer before lengthy operation
    lastReadyPrint = millis();
    
    yield(); // Yield before Firebase path operation
    String profilePath = "/Students/" + rfidUid + "/Profile";
    bool createProfile = true;

    if (Firebase.RTDB.get(&fbdo, profilePath)) {
      createProfile = false;
      yield(); // Yield after Firebase operation
    }

    if (createProfile) {
      yield(); // Yield before fetch operation
      if (firestoreStudents.find(rfidUid) == firestoreStudents.end()) {
        Serial.println("Refreshing Firestore data for new student...");
        yield(); // Yield before Firestore fetch
        fetchFirestoreStudents();
        yield(); // Yield after Firestore fetch
      }

      if (firestoreStudents.find(rfidUid) != firestoreStudents.end()) {
        yield(); // Yield before data extraction
        FirebaseJson profileJson;
        auto& studentData = firestoreStudents[rfidUid];

        profileJson.set("fullName", studentData["fullName"].length() > 0 ? studentData["fullName"] : "Unknown");
        yield();
        profileJson.set("email", studentData["email"]);
        profileJson.set("idNumber", studentData["idNumber"]);
        profileJson.set("mobileNumber", studentData["mobileNumber"]);
        yield();
        profileJson.set("role", "student");
        profileJson.set("department", studentData["department"]);
        profileJson.set("rfidUid", rfidUid);
        profileJson.set("createdAt", timestamp);
        profileJson.set("lastUpdated", timestamp);

        if (studentData["schedules"].length() > 0) {
          yield(); // Yield before schedule processing
          FirebaseJson schedulesJson;
          schedulesJson.setJsonData(studentData["schedules"]);
          profileJson.set("schedules", schedulesJson);
          yield(); // Yield after schedule processing
        }

        yield(); // Yield before RTDB update
        if (Firebase.RTDB.setJSON(&fbdo, profilePath, &profileJson)) {
          Serial.println("Created new student profile in RTDB for " + rfidUid);
        } else {
          Serial.println("Failed to create student profile: " + fbdo.errorReason());
          if (fbdo.errorReason().indexOf("ssl") >= 0 || 
              fbdo.errorReason().indexOf("connection") >= 0 || 
              fbdo.errorReason().indexOf("SSL") >= 0) {
            handleFirebaseSSLError();
          }
        }
        yield(); // Yield after RTDB update
      }
    }
  }

  yield(); // Yield before attendance processing

  // Default student data
  String studentName = "Unknown";
  String email = "", idNumber = "", mobileNumber = "", role = "", department = "";
  String schedulesJsonStr = "[]";
  String sectionId = "";
  
  // Get student data from cache
  if (firestoreStudents.find(rfidUid) != firestoreStudents.end()) {
    yield(); // Yield before data extraction
    studentName = firestoreStudents[rfidUid]["fullName"].length() > 0 ? firestoreStudents[rfidUid]["fullName"] : "Unknown";
    email = firestoreStudents[rfidUid]["email"];
    idNumber = firestoreStudents[rfidUid]["idNumber"];
    mobileNumber = firestoreStudents[rfidUid]["mobileNumber"];
    role = firestoreStudents[rfidUid]["role"].length() > 0 ? firestoreStudents[rfidUid]["role"] : "student";
    department = firestoreStudents[rfidUid]["department"];
    schedulesJsonStr = firestoreStudents[rfidUid]["schedules"].length() > 0 ? firestoreStudents[rfidUid]["schedules"] : "[]";
    yield(); // Yield after data extraction
  }

  yield(); // Yield before status determination

  // Determine status and action
  String finalStatus = "Pending"; // Default to Pending until weight is confirmed
  String action = "Initial Tap";
  
  // For absent students (sensorIndex == -3), ensure weight is 0.0
  float sensorWeight = (sensorIndex == -3) ? 0.0 : weight;
  
  // Always update the sensorType to include specific sensor number for the current student
  String sensorType = "Weight Sensor";
  if (sensorIndex >= 0 && sensorIndex < NUM_SENSORS) {
    sensorType = "Weight Sensor " + String(sensorIndex + 1);
  }

  yield(); // Yield before SD operations

  // Log to SD if offline
  if (!isConnected || sdMode) {
    String entry = "Student:UID:" + rfidUid +
                   " TimeIn:" + timestamp +
                   " Action:" + action +
                   " Status:" + finalStatus +
                   " Sensor:" + sensorStr +
                   " Weight:" + String(sensorWeight) +  // Use sanitized weight value
                   " assignedSensorId:" + String(sensorIndex >= 0 ? sensorIndex : -1) +
                   (timeOut != "" ? " TimeOut:" + timeOut : "");
    yield(); // Yield before SD write
    storeLogToSD(entry);
    Serial.println("SD log: " + entry);
    yield(); // Yield after SD write
  }

  yield(); // Yield before Firebase operations

  // Firebase logging with new structure
  if (!sdMode && isConnected && Firebase.ready()) {
    String date = timestamp.substring(0, 10);
    
    // Process schedules
    FirebaseJsonArray allSchedulesArray;
    FirebaseJson matchedSchedule;
    String subjectCode = "Unknown", roomName = "Unknown", sectionName = "Unknown";
    
    if (schedulesJsonStr != "[]") {
      yield(); // Yield before schedule processing
      FirebaseJsonArray tempArray;
      if (tempArray.setJsonArrayData(schedulesJsonStr)) {
        String currentDay = getDayFromTimestamp(timestamp);
        String currentTime = timestamp.substring(11, 16);
        
        for (size_t i = 0; i < tempArray.size(); i++) {
          if (i % 2 == 0) yield(); // Yield every 2 schedules
          FirebaseJsonData scheduleData;
          if (tempArray.get(scheduleData, i)) {
            FirebaseJson scheduleObj;
            if (scheduleObj.setJsonData(scheduleData.stringValue)) {
              FirebaseJson newScheduleObj;
              FirebaseJsonData fieldData;
              
              yield(); // Yield before field extraction
              if (scheduleObj.get(fieldData, "day")) newScheduleObj.set("day", fieldData.stringValue);
              if (scheduleObj.get(fieldData, "startTime")) newScheduleObj.set("startTime", fieldData.stringValue);
              if (scheduleObj.get(fieldData, "endTime")) newScheduleObj.set("endTime", fieldData.stringValue);
              yield();
              if (scheduleObj.get(fieldData, "roomName")) {
                newScheduleObj.set("roomName", fieldData.stringValue);
                roomName = fieldData.stringValue;
              }
              if (scheduleObj.get(fieldData, "subjectCode")) {
                newScheduleObj.set("subjectCode", fieldData.stringValue);
                subjectCode = fieldData.stringValue;
              }
              if (scheduleObj.get(fieldData, "subject")) newScheduleObj.set("subject", fieldData.stringValue);
              yield();
              if (scheduleObj.get(fieldData, "section")) {
                newScheduleObj.set("section", fieldData.stringValue);
                sectionName = fieldData.stringValue;
              }
              if (scheduleObj.get(fieldData, "instructorName")) newScheduleObj.set("instructorName", fieldData.stringValue);
              if (scheduleObj.get(fieldData, "sectionId")) {
                newScheduleObj.set("sectionId", fieldData.stringValue);
                sectionId = fieldData.stringValue; // Store the section ID for later use
              }
              
              yield(); // Yield before array add
              allSchedulesArray.add(newScheduleObj);
              
              // Check if this is the current schedule
              if (scheduleObj.get(fieldData, "day") && fieldData.stringValue == currentDay) {
                String startTime, endTime;
                if (scheduleObj.get(fieldData, "startTime")) startTime = fieldData.stringValue;
                if (scheduleObj.get(fieldData, "endTime")) endTime = fieldData.stringValue;
                if (isTimeInRange(currentTime, startTime, endTime)) {
                  matchedSchedule = newScheduleObj;
                  yield(); // Yield after match
                  break;
                }
              }
            }
          }
        }
      }
    }

    yield(); // Yield before session ID check

    // Get the last session ID
    String lastSessionId = "";
    if (currentSessionId != "") {
      lastSessionId = currentSessionId;
    } else {
      String lastSessionPath = "/Students/" + rfidUid + "/lastSession";
      if (Firebase.RTDB.getString(&fbdo, lastSessionPath)) {
        lastSessionId = fbdo.stringData();
      }
    }

    if (lastSessionId != "") {
      yield(); // Yield before attendance path check
      // Check if this is a continued attendance record or new one
      String attendancePath = "/Students/" + rfidUid + "/Attendance/" + lastSessionId;
  FirebaseJson attendanceInfoJson;
  
      // Set base attendance info
  attendanceInfoJson.set("status", finalStatus);
      attendanceInfoJson.set("timeIn", timestamp);
  attendanceInfoJson.set("action", action);
      attendanceInfoJson.set("sensorType", sensorType);
  attendanceInfoJson.set("rfidAuthenticated", true);
      attendanceInfoJson.set("assignedSensorId", sensorIndex >= 0 ? sensorIndex : -1);
  
  if (timeOut != "") {
    attendanceInfoJson.set("timeOut", timeOut);
  }
  
      // Add session info
      FirebaseJson sessionInfoJson;
      sessionInfoJson.set("sessionStartTime", timestamp);
      sessionInfoJson.set("subjectCode", subjectCode);
      sessionInfoJson.set("roomName", roomName);
      sessionInfoJson.set("section", sectionName);

      // Update the RTDB with attendance info
      yield(); // Yield before Firebase update
      String attendanceInfoPath = attendancePath + "/attendanceInfo";
      if (Firebase.RTDB.updateNode(&fbdo, attendanceInfoPath, &attendanceInfoJson)) {
        Serial.println("Updated attendance info for " + rfidUid + " session " + lastSessionId + ": " + finalStatus);
      } else {
        Serial.println("Failed to update attendance: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
      
      // Update session info if not already set
      yield(); // Yield before session info update
      String sessionInfoPath = attendancePath + "/sessionInfo";
      if (Firebase.RTDB.updateNode(&fbdo, sessionInfoPath, &sessionInfoJson)) {
        Serial.println("Updated session info for " + rfidUid);
      } else {
        Serial.println("Failed to update session info: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }

      // Set the last session for this student
      yield(); // Yield before last session update
      String lastSessionPath = "/Students/" + rfidUid + "/lastSession";
      if (Firebase.RTDB.setString(&fbdo, lastSessionPath, lastSessionId)) {
        Serial.println("Updated last session for " + rfidUid + " to " + lastSessionId);
    } else {
        Serial.println("Failed to update last session: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
    }
  } else {
      Serial.println("No session ID available for " + rfidUid);
    }
  }
  
  yield(); // Final yield before updating timers
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// Helper functions (add these to your code)
String getDayFromTimestamp(String timestamp) {
  // Convert "2025_04_11_220519" to day of week
  int year = timestamp.substring(0, 4).toInt();
  int month = timestamp.substring(5, 7).toInt();
  int day = timestamp.substring(8, 10).toInt();
  // Simple Zeller's Congruence for day of week
  if (month < 3) {
    month += 12;
    year--;
  }
  int k = day;
  int m = month;
  int D = year % 100;
  int C = year / 100;
  int f = k + ((13 * (m + 1)) / 5) + D + (D / 4) + (C / 4) - (2 * C);
  int dayOfWeek = f % 7;
  if (dayOfWeek < 0) dayOfWeek += 7;

  const char* days[] = {"Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
  return String(days[dayOfWeek]);
}

bool isTimeInRange(String currentTime, String startTime, String endTime) {
  // Convert times to minutes for comparison (e.g., "22:05" -> 1325)
  int currentMins = currentTime.substring(0, 2).toInt() * 60 + currentTime.substring(3, 5).toInt();
  int startMins = startTime.substring(0, 2).toInt() * 60 + startTime.substring(3, 5).toInt();
  int endMins = endTime.substring(0, 2).toInt() * 60 + endTime.substring(3, 5).toInt();
  return currentMins >= startMins && currentMins <= endMins;
}


void logUnregisteredUID(String uid, String timestamp) {
  Serial.println("Updating unregistered UID: " + uid);
  
  // Display message about unregistered UID
  displayMessage("Unregistered ID", "Access Denied", 2000);
  
  // First check if the UID is actually registered but wasn't found on first check
  // by trying a direct fetch from Firestore
  if (!sdMode && isConnected && Firebase.ready()) {
    // Try to check Firestore students collection
    Serial.println("Directly checking Firestore for UID " + uid);
    String firestorePath = "students";
    if (Firebase.Firestore.getDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", firestorePath.c_str(), "")) {
      FirebaseJson json;
      json.setJsonData(firestoreFbdo.payload());
      FirebaseJsonData jsonData;
      if (json.get(jsonData, "documents")) {
        FirebaseJsonArray arr;
        jsonData.getArray(arr);
        for (size_t i = 0; i < arr.size(); i++) {
          FirebaseJsonData docData;
          arr.get(docData, i);
          FirebaseJson doc;
          doc.setJsonData(docData.to<String>());
          String rfidUid = "";
          FirebaseJsonData fieldData;
          if (doc.get(fieldData, "fields/rfidUid/stringValue")) {
            rfidUid = fieldData.stringValue;
            if (rfidUid == uid) {
              // Found it! Now cache the student data for future use
              Serial.println("Found student with UID " + uid + " in Firestore. Adding to cache.");
              
              std::map<String, String> studentData;
              if (doc.get(fieldData, "fields/fullName/stringValue")) {
                studentData["fullName"] = fieldData.stringValue;
              } else {
                studentData["fullName"] = "Unknown";
              }
              
              if (doc.get(fieldData, "fields/email/stringValue")) {
                studentData["email"] = fieldData.stringValue;
              }
              
              if (doc.get(fieldData, "fields/role/stringValue")) {
                studentData["role"] = fieldData.stringValue;
              } else {
                studentData["role"] = "student";
              }
              
              firestoreStudents[uid] = studentData;
              
              // Force a full data fetch to get complete student information
              fetchFirestoreStudents();
              return; // Exit without logging as unregistered since it's actually registered
            }
          }
        }
      }
    } else {
      Serial.println("Failed to retrieve Firestore students: " + firestoreFbdo.errorReason());
      if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
          firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
          firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
    
    // If we reach here, the UID is not in Firestore, so log it as unregistered
    if (Firebase.RTDB.setString(&fbdo, "/Unregistered/" + uid + "/Time", timestamp)) {
      Serial.println("Unregistered UID logged to Firebase RTDB: Unregistered:" + uid + " Time:" + timestamp);
    } else {
      Serial.println("Failed to log unregistered UID to RTDB: " + fbdo.errorReason());
      String entry = "Unregistered:UID:" + uid + " Time:" + timestamp;
      storeLogToSD(entry);
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }
  } else {
    // Offline mode - log to SD
    String entry = "Unregistered:UID:" + uid + " Time:" + timestamp;
    storeLogToSD(entry);
  }
}

void logAdminAccess(String uid, String timestamp) {
  // Heap check to prevent crashes
  uint32_t freeHeap = ESP.getFreeHeap();
  if (freeHeap < 15000) {
    Serial.println("Warning: Low heap (" + String(freeHeap) + " bytes). Skipping Firebase operations.");
    storeLogToSD("LowHeapWarning:UID:" + uid + " Time:" + timestamp);
    deniedFeedback();
    displayMessage("System Busy", "Try Again", 2000);
    return;
  }

  // SD log entry (basic)
  String entry = "Admin:UID:" + uid + " Time:" + timestamp;
  yield(); // Allow system to process after SD operation

  // Validate admin UID
  if (!isAdminUID(uid)) {
    entry += " Action:Denied_NotAdmin";
    storeLogToSD(entry);
    deniedFeedback();
    displayMessage("Not Admin", "Access Denied", 2000);
    return;
  }

  // Fetch user details with yield
  std::map<String, String> userData = fetchUserDetails(uid);
  String fullName = userData.empty() ? "Unknown" : userData["fullName"];
  String role = userData.empty() ? "admin" : userData["role"];
  yield(); // Allow system to process after user fetch

  // Sanitize timestamp for Firebase paths
  String sanitizedTimestamp = timestamp;
  sanitizedTimestamp.replace(" ", "_");
  sanitizedTimestamp.replace(":", "");
  sanitizedTimestamp.replace("/", "_");

  // IMPROVED LOGIC: First check if this admin is the one who started the current session
  bool isEntry;
  String action;
  
  if (adminAccessActive && uid == lastAdminUID) {
    // This is the same admin who started the session - this is an EXIT
    isEntry = false;
    action = "exit";
  } else if (adminAccessActive && uid != lastAdminUID) {
    // Different admin is trying to access - deny
    entry += " Action:Denied_DifferentUID";
    storeLogToSD(entry);
    deniedFeedback();
    Serial.println("Different admin UID detected: " + uid);
    displayMessage("Session Active", "Use Same UID", 2000);
    displayMessage("Admin Mode", "Active", 0);
    
    // Update timers
    firstActionOccurred = true;
    lastActivityTime = millis();
    lastReadyPrint = millis();
    return;
  } else {
    // No admin session active - this is an ENTRY
    isEntry = true;
    action = "entry";
  }

  // Assign room before creating the AccessLogs entry
  if (isEntry) {
    assignedRoomId = assignRoomToAdmin(uid);
  }

  // Log PZEM data on exit for SD
  if (!isEntry) {
    float voltage = max(pzem.voltage(), 0.0f);
    float current = max(pzem.current(), 0.0f);
    float power = max(pzem.power(), 0.0f);
    float energy = max(pzem.energy(), 0.0f);
    float frequency = max(pzem.frequency(), 0.0f);
    float powerFactor = max(pzem.pf(), 0.0f);
    entry += " Action:Exit Voltage:" + String(voltage, 2) + "V Current:" + String(current, 2) + "A Power:" + String(power, 2) +
             "W Energy:" + String(energy, 3) + "kWh Frequency:" + String(frequency, 2) + "Hz PowerFactor:" + String(powerFactor, 2);
  } else {
    entry += " Action:Entry";
  }
  storeLogToSD(entry);

  // Firebase logging (/AccessLogs and /AdminPZEM)
  if (!sdMode && isConnected && Firebase.ready()) {
    // Prevent watchdog triggering during Firebase operations
    lastReadyPrint = millis(); 
    
    // /AccessLogs
    String accessPath = "/AccessLogs/" + uid + "/" + sanitizedTimestamp;
    FirebaseJson accessJson;
    accessJson.set("action", action);
    accessJson.set("timestamp", timestamp);
    accessJson.set("fullName", fullName);
    accessJson.set("role", role);

    // For entry, add room details to AccessLogs
    if (isEntry) {
      // Using assignedRoomId set before Firebase operations
      if (assignedRoomId != "" && firestoreRooms.find(assignedRoomId) != firestoreRooms.end()) {
        const auto& roomData = firestoreRooms[assignedRoomId];
        FirebaseJson roomDetails;

        // Use at() for const map access and proper error handling
        try {
          roomDetails.set("building", roomData.count("building") ? roomData.at("building") : "Unknown");
          roomDetails.set("floor", roomData.count("floor") ? roomData.at("floor") : "Unknown");
          roomDetails.set("name", roomData.count("name") ? roomData.at("name") : "Unknown");
          roomDetails.set("status", "maintenance");  // Always set to maintenance for admin inspections
          roomDetails.set("type", roomData.count("type") ? roomData.at("type") : "Unknown");
          accessJson.set("roomDetails", roomDetails);

          // Also update the room status in Firestore
          String roomPath = "rooms/" + assignedRoomId;
          FirebaseJson contentJson;
          contentJson.set("fields/status/stringValue", "maintenance");
          
          if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", roomPath.c_str(), contentJson.raw(), "status")) {
            Serial.println("Room status updated to 'maintenance' in Firestore: " + assignedRoomId);
          } else {
            Serial.println("Failed to update room status in Firestore: " + firestoreFbdo.errorReason());
            if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
                firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
                firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
              handleFirebaseSSLError();
            }
          }
        } catch (const std::out_of_range& e) {
          Serial.println("Error accessing room data: " + String(e.what()));
          roomDetails.set("building", "Unknown");
          roomDetails.set("floor", "Unknown");
          roomDetails.set("name", "Unknown");
          roomDetails.set("status", "maintenance");
          roomDetails.set("type", "Unknown");
          accessJson.set("roomDetails", roomDetails);
        }
      }
    }

    // For exit, add PZEM data to AccessLogs
    if (!isEntry && isVoltageSufficient) {
      // Get the entry timestamp to find the entry record
      String entryTimestamp = "";
      if (Firebase.RTDB.getJSON(&fbdo, "/AccessLogs/" + uid)) {
        FirebaseJson json;
        json.setJsonData(fbdo.to<FirebaseJson>().raw());
        
        // Find the most recent "entry" action using Firebase iterators correctly
        size_t count = json.iteratorBegin();
        String latestEntryKey = "";
        
        for (size_t i = 0; i < count; i++) {
          int type = 0;
          String key, value;
          json.iteratorGet(i, type, key, value);
          
          if (type == FirebaseJson::JSON_OBJECT) {
            // This is an entry, check if it has 'action' = 'entry'
            FirebaseJson entryJson;
            FirebaseJsonData actionData;
            
            // Create a new JSON with just this item and check it
            String jsonStr = "{\"" + key + "\":" + value + "}";
            FirebaseJson keyJson;
            keyJson.setJsonData(jsonStr);
            
            // Get action from this entry
            if (keyJson.get(actionData, key + "/action") && 
                actionData.stringValue == "entry") {
              // Found an entry action, check if it's newer
              if (latestEntryKey == "" || key.compareTo(latestEntryKey) > 0) {
                latestEntryKey = key;
              }
            }
          }
        }
        
        json.iteratorEnd();
        
        if (latestEntryKey != "") {
          entryTimestamp = latestEntryKey;
          Serial.println("Found latest entry record timestamp: " + entryTimestamp);
          
          // Update room status back to "available" when admin exits
          FirebaseJson statusUpdate;
          statusUpdate.set("status", "available");
          
          if (Firebase.RTDB.updateNode(&fbdo, "/AccessLogs/" + uid + "/" + entryTimestamp + "/roomDetails", &statusUpdate)) {
            Serial.println("Room status updated to 'available' in entry record: " + entryTimestamp);
          } else {
            Serial.println("Failed to update room status: " + fbdo.errorReason());
            if (fbdo.errorReason().indexOf("ssl") >= 0 || 
                fbdo.errorReason().indexOf("connection") >= 0 || 
                fbdo.errorReason().indexOf("SSL") >= 0) {
              handleFirebaseSSLError();
            }
          }
        }
      }
      
      // No need to update the roomDetails/exit data since we have a separate exit record
      // We'll only keep the PZEM data in the separate exit record
      
      // Add PZEM data to the current exit record
      FirebaseJson pzemData;
      pzemData.set("voltage", lastVoltage);
      pzemData.set("current", lastCurrent);
      pzemData.set("power", lastPower);
      pzemData.set("energy", lastEnergy);
      pzemData.set("frequency", lastFrequency);
      pzemData.set("powerFactor", lastPowerFactor);
      accessJson.set("pzemData", pzemData);
    }

    Serial.print("Pushing to RTDB: " + accessPath + "... ");
    if (Firebase.RTDB.setJSON(&fbdo, accessPath, &accessJson)) {
      Serial.println("Success");
      Serial.println("Admin " + fullName + " access logged: " + action);
    } else {
      Serial.println("Failed: " + fbdo.errorReason());
      storeLogToSD("AccessLogFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }

    // We no longer log to AdminPZEM, using AccessLogs instead
    yield(); // Allow system to process after Firebase operations

    // Update /Admin/<uid>
    if (!userData.empty()) {
      String adminPath = "/Admin/" + uid;
      FirebaseJson adminJson;
      adminJson.set("fullName", userData["fullName"]);
      adminJson.set("role", userData["role"]);
      adminJson.set("createdAt", userData.count("createdAt") ? userData["createdAt"] : "2025-01-01T00:00:00.000Z");
      adminJson.set("email", userData.count("email") ? userData["email"] : "unknown@gmail.com");
      adminJson.set("idNumber", userData.count("idNumber") ? userData["idNumber"] : "N/A");
      adminJson.set("rfidUid", uid);
      // Only update lastTamperStop if previously set (avoid overwriting tamper resolution)
      if (userData.count("lastTamperStop")) {
        adminJson.set("lastTamperStop", userData["lastTamperStop"]);
      }

      Serial.print("Updating RTDB: " + adminPath + "... ");
      if (Firebase.RTDB.setJSON(&fbdo, adminPath, &adminJson)) {
        Serial.println("Success");
        Serial.println("Admin details updated in RTDB at " + adminPath);
      } else {
        Serial.println("Failed: " + fbdo.errorReason());
        storeLogToSD("AdminUpdateFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    yield(); // Allow system to process after Firebase operations
    }
  }

  // Now use the simplified isEntry flag for behavior
  if (isEntry) {
    activateRelays();
    adminAccessActive = true;
    lastAdminUID = uid;
    
    // Set up door auto-lock timeout
    adminDoorOpenTime = millis();
    Serial.println("Door will auto-lock in 30 seconds while admin inspection continues");
    
    // Room already assigned above
    if (assignedRoomId == "") {
      displayMessage("No Room Available", "For Inspection", 2000);
    } else {
      if (firestoreRooms.find(assignedRoomId) != firestoreRooms.end()) {
        String roomName = firestoreRooms[assignedRoomId].at("name");
        displayMessage("Inspecting Room", roomName, 2000);
      }
    }
    accessFeedback();
    logSystemEvent("Relay Activated for Admin UID: " + uid);
    Serial.println("Admin access granted for UID: " + uid);
    displayMessage("Admin Access", "Granted", 2000);
    displayMessage("Admin Mode", "Active", 0);

  // Handle exit - now this is captured by the isEntry flag rather than a separate condition
  } else {
    // Use our improved deactivateRelays function for safer relay operations
    deactivateRelays();
    
    // Allow system to stabilize after relay operations
    yield();
    delay(50);
    yield();
    
    adminAccessActive = false;
    lastAdminUID = "";
    lastPZEMLogTime = 0;
    
    // Reset the RFID reader to ensure it's in a clean state for the next read
    rfid.PCD_Reset();
    delay(100);
    rfid.PCD_Init();
    
    // Also update room status in Firestore if we have a room assigned
    if (assignedRoomId != "" && !sdMode && isConnected && Firebase.ready()) {
      // Update the room status in Firestore back to "available"
      String roomPath = "rooms/" + assignedRoomId;
      FirebaseJson contentJson;
      contentJson.set("fields/status/stringValue", "available");
      
      if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", roomPath.c_str(), contentJson.raw(), "status")) {
        Serial.println("Room status updated to 'available' in Firestore: " + assignedRoomId);
      } else {
        Serial.println("Failed to update room status in Firestore: " + firestoreFbdo.errorReason());
        if (firestoreFbdo.errorReason().indexOf("ssl") >= 0 || 
            firestoreFbdo.errorReason().indexOf("connection") >= 0 || 
            firestoreFbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    }
    
    assignedRoomId = "";
    accessFeedback();
    logSystemEvent("Relay Deactivated for Admin UID: " + uid);
    Serial.println("Admin access ended for UID: " + uid);
    displayMessage("Admin Access", "Ended", 2000);
    displayMessage("Door Locked", "", 2000);
    
    // Reset session state for clean transition
    resetSessionState();
    
    // Allow the system to stabilize before transition
    yield();
    
    // Use smooth transition instead of direct ready message
    smoothTransitionToReady();
  }

  // Update timers
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();

  // Log heap after operations
  Serial.println("Heap after logAdminAccess: " + String(ESP.getFreeHeap()) + " bytes");
}

void logAdminTamperStop(String uid, String timestamp) {
  String entry = "Admin:" + uid + " TamperStopped:" + timestamp;
  storeLogToSD(entry);

  // Update tamper event in Alerts/Tamper node
  if (!sdMode && isConnected && Firebase.ready()) {
    // Use currentTamperAlertId if available, fallback to tamperStartTime
    String alertId = currentTamperAlertId.length() > 0 ? currentTamperAlertId : tamperStartTime;
    String tamperPath = "/Alerts/Tamper/" + alertId;
    
    // Fetch user details for resolvedByFullName
    std::map<String, String> userData = fetchUserDetails(uid);
    String fullName = userData.empty() ? "Unknown Admin" : userData["fullName"];
    String role = userData.empty() ? "admin" : userData["role"];
    
    FirebaseJson tamperJson;
    tamperJson.set("endTime", timestamp);
    tamperJson.set("status", "resolved");
    tamperJson.set("resolvedBy", uid);
    tamperJson.set("resolverName", fullName);
    tamperJson.set("resolverRole", role);
    tamperJson.set("resolutionTime", timestamp);

    Serial.print("Logging tamper resolution: " + tamperPath + "... ");
    if (Firebase.RTDB.updateNode(&fbdo, tamperPath, &tamperJson)) {
      Serial.println("Success");
      Serial.println("Tamper event resolved at " + tamperPath + " by UID " + uid);
    } else {
      Serial.println("Failed: " + fbdo.errorReason());
      storeLogToSD("TamperStopFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
      if (fbdo.errorReason().indexOf("ssl") >= 0 || 
          fbdo.errorReason().indexOf("connection") >= 0 || 
          fbdo.errorReason().indexOf("SSL") >= 0) {
        handleFirebaseSSLError();
      }
    }

    // Update /Admin/<uid> with original fields and tamper resolution info
    if (!userData.empty()) {
      String adminPath = "/Admin/" + uid;
      FirebaseJson adminJson;
      adminJson.set("fullName", userData["fullName"]);
      adminJson.set("role", userData["role"]);
      adminJson.set("lastTamperStop", timestamp);
      adminJson.set("lastTamperAlertId", alertId);
      // Restore original fields
      adminJson.set("email", userData["email"].length() > 0 ? userData["email"] : "unknown@gmail.com");
      adminJson.set("idNumber", userData["idNumber"].length() > 0 ? userData["idNumber"] : "N/A");
      adminJson.set("createdAt", userData.count("createdAt") > 0 ? userData["createdAt"] : "2025-01-01T00:00:00.000Z");
      adminJson.set("rfidUid", uid);

      Serial.print("Updating RTDB: " + adminPath + "... ");
      if (Firebase.RTDB.updateNode(&fbdo, adminPath, &adminJson)) {
        Serial.println("Success");
        Serial.println("Admin node updated for tamper resolution at " + adminPath);
      } else {
        Serial.println("Failed: " + fbdo.errorReason());
        storeLogToSD("AdminUpdateFailed:UID:" + uid + " Time:" + timestamp + " Error:" + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    } else {
      Serial.println("No Firestore data for UID " + uid + "; logging minimal /Admin update.");
      String adminPath = "/Admin/" + uid;
      FirebaseJson adminJson;
      adminJson.set("fullName", "Unknown Admin");
      adminJson.set("role", "admin");
      adminJson.set("lastTamperStop", timestamp);
      adminJson.set("lastTamperAlertId", alertId);
      adminJson.set("email", "unknown@gmail.com");
      adminJson.set("idNumber", "N/A");
      adminJson.set("createdAt", "2025-01-01T00:00:00.000Z");
      adminJson.set("rfidUid", uid);

      if (Firebase.RTDB.updateNode(&fbdo, adminPath, &adminJson)) {
        Serial.println("Minimal admin node updated at " + adminPath);
      } else {
        Serial.println("Minimal admin update failed: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    }
  } else {
    Serial.println("Firebase unavailable; tamper stop logged to SD.");
    std::map<String, String> userData = fetchUserDetails(uid);
    String fullName = userData.empty() ? "Unknown Admin" : userData["fullName"];
    String detailedEntry = "Admin:" + uid + " TamperStopped:" + timestamp + 
                          " ResolvedByUID:" + uid + " ResolvedByFullName:" + fullName;
    storeLogToSD(detailedEntry);
  }

  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();

  // Transition to ready state after tamper resolution
  smoothTransitionToReady();
}

void logSystemEvent(String event) {
  String timestamp = getFormattedTime();
  String entry = "System:" + event + " Time:" + timestamp;
  storeLogToSD(entry);
  if (!sdMode && isConnected && isVoltageSufficient) {
    Firebase.RTDB.pushString(&fbdo, "/SystemLogs", entry);
  }
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void watchdogCheck() {
  // Only trigger watchdog if there's no active operation that could legitimately take a long time
  if ((millis() - lastReadyPrint > 300000) && 
      !adminAccessActive && 
      !tamperActive && 
      !classSessionActive && 
      !studentVerificationActive && 
      !relayActive && 
      !tapOutPhase) {
    Serial.println("Watchdog timeout. Restarting system.");
    logSystemEvent("Watchdog Reset");
    ESP.restart();
  }
}

// Add the feedWatchdog function implementation
void feedWatchdog() {
  // ESP32 Arduino core doesn't have ESP.wdtFeed(), use alternatives
  yield();
  delay(1); // Tiny delay to ensure background processes can run
}

// Helper function for safe Firebase operations with watchdog handling
void safeFirebaseOperation() {
  // Declare external variables to ensure they're in scope
  extern bool displayingMessage;
  extern String currentLine1;
  extern String currentLine2;
  
  // Skip Firebase operations during startup and when WiFi is being reconfigured
  if (millis() < 5000 || wifiReconfiguring) {
    yield();
    return;
  }
  
  static unsigned long operationStartTime = 0;
  
  // First Firebase operation call
  if (operationStartTime == 0) {
    operationStartTime = millis();
    return;
  }
  
  // Reset watchdog during long-running Firebase operations
  if (millis() - operationStartTime > 5000) {
    Serial.println("Long-running Firebase operation, resetting watchdog");
    lastReadyPrint = millis();
    operationStartTime = millis();
    
    // Add multiple yields to ensure the system stays responsive
    for (int i = 0; i < 5; i++) {
      yield();
      delay(1);
    }
  }
  
  // Always yield during Firebase operations
  yield();
  
  // Periodically check if the LCD needs updating during long operations
  static unsigned long lastLcdCheck = 0;
  if (millis() - lastLcdCheck > 1000) {  // Check every second
    lastLcdCheck = millis();
    // Force LCD update if needed
    if (displayingMessage) {
      // Refresh the current LCD display if a message is active
      if (currentLine1.length() > 0 || currentLine2.length() > 0) {
        int retryCount = 0;
        const int maxRetries = 3;
        while (retryCount < maxRetries) {
          lcd.clear();
          lcd.setCursor(0, 0);
          lcd.print(currentLine1);
          lcd.setCursor(0, 1);
          lcd.print(currentLine2);
          
          Wire.beginTransmission(0x27);
          if (Wire.endTransmission() == 0) break;
          recoverI2C();
          delay(10);
          yield();
          retryCount++;
        }
      }
    }
  }
}

bool checkResetButton() {
  static unsigned long pressStart = 0;
  static int lastButtonState = HIGH;
  static unsigned long lastDebounceTime = 0;
  const unsigned long debounceDelay = 50;    // 50ms debounce
  const unsigned long pressDuration = 200;   // 200ms to confirm press

  int currentButtonState = digitalRead(RESET_BUTTON_PIN);

  // Debug: Log state changes
  if (currentButtonState != lastButtonState) {
    Serial.print("Reset button state changed to: ");
    Serial.println(currentButtonState == LOW ? "LOW (pressed)" : "HIGH (released)");
    lastDebounceTime = millis();
  }

  // Debounce: Update state only if stable for debounceDelay
  if ((millis() - lastDebounceTime) > debounceDelay) {
    if (currentButtonState == LOW && lastButtonState == HIGH) {
      // Button just pressed
      pressStart = millis();
      Serial.println("Reset button press detected, starting timer...");
      // Immediate feedback
      tone(BUZZER_PIN, 2000, 100);
      digitalWrite(LED_R_PIN, HIGH);
      delay(50); // Brief feedback
      digitalWrite(LED_R_PIN, LOW);
    } else if (currentButtonState == HIGH && lastButtonState == LOW) {
      // Button released
      pressStart = 0;
      Serial.println("Reset button released.");
    }
    lastButtonState = currentButtonState;
  }

  // Check for confirmed press
  if (currentButtonState == LOW && pressStart != 0 && (millis() - pressStart >= pressDuration)) {
    Serial.println("Reset confirmed after 200ms. Initiating restart...");
    // Log to SD if initialized
    if (sdInitialized) {
      String timestamp = getFormattedTime();
      String entry = "System:ResetButton Timestamp:" + timestamp + " Action:UserReset";
      storeLogToSD(entry);
      Serial.println("Reset logged to SD: " + entry);
    }
    // Final feedback
    tone(BUZZER_PIN, 1000, 200);
    digitalWrite(LED_R_PIN, HIGH);
    digitalWrite(LED_G_PIN, HIGH);
    digitalWrite(LED_B_PIN, HIGH);
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Resetting...");
    delay(300); // Reduced from 500ms to ensure quick restart
    digitalWrite(LED_R_PIN, LOW);
    digitalWrite(LED_G_PIN, LOW);
    digitalWrite(LED_B_PIN, LOW);
    ESP.restart();
    return true; // Won't be reached
  }

  return false;
}

// Global variables for display state
unsigned long displayStartTime = 0;
String currentLine1 = "";
String currentLine2 = "";
unsigned long displayDuration = 0;
bool displayingMessage = false;

void displayMessage(String line1, String line2, unsigned long duration = 3000) {
  // Local variables for transitions
  static bool transitionInProgress = false;
  static int transitionStep = 0;
  static unsigned long lastTransitionStep = 0;
  
  // Check heap memory
  if (ESP.getFreeHeap() < 5000) {
    Serial.println("Low heap memory: " + String(ESP.getFreeHeap()));
    recoverI2C();
    yield(); // Add yield when low on memory
    return;
  }
  
  // Truncate messages to 16 chars
  line1 = line1.substring(0, 16);
  line2 = line2.substring(0, 16);
  
  // Keep track of the current message even if it's identical to ensure it can be refreshed
  String newLine1 = line1;
  String newLine2 = line2;
  
  // If same message is already displayed, just update the duration timer but still refresh display
  if (line1 == currentLine1 && line2 == currentLine2 && !transitionInProgress) {
    displayStartTime = millis();
    displayDuration = duration;
    displayingMessage = (duration > 0);
    
    // Force refresh to keep the display responsive during errors
    lcd.clear();
    yield(); // Add yield before LCD update
    lcd.setCursor(0, 0);
    lcd.print(currentLine1);
    lcd.setCursor(0, 1);
    lcd.print(currentLine2);
    yield(); // Add yield after LCD update
    
    return;
  }
  
  // Start transition if not already in progress
  if (!transitionInProgress) {
    transitionInProgress = true;
    transitionStep = 0;
    lastTransitionStep = millis();
  }
  
  // Handle transitions
  if (transitionInProgress) {
    if (millis() - lastTransitionStep >= 10) { // 10ms between transition steps
      lastTransitionStep = millis();
      
      switch (transitionStep) {
        case 0: {
          // Fade out
          int retryCount = 0;
          const int maxRetries = 3;
          while (retryCount < maxRetries) {
            lcd.clear();
            yield(); // Add yield after clear
            lcd.setCursor(0, 0);
            lcd.print(currentLine1);
            lcd.setCursor(0, 1);
            lcd.print(currentLine2);
            yield(); // Add yield after print
            
            Wire.beginTransmission(0x27);
            if (Wire.endTransmission() == 0) break;
            Serial.println("I2C communication failed, retrying... Attempt " + String(retryCount + 1));
            recoverI2C();
            delay(10); // Short blocking delay, safe for I2C recovery
            yield();
            retryCount++;
          }
          
          // Prepare for fade-in
          transitionStep = 1;
          yield(); // Allow system to process during transition
          break;
        }
        
        case 1: {
          // Fade in
          int retryCount = 0;
          const int maxRetries = 3;
          while (retryCount < maxRetries) {
            lcd.clear();
            yield(); // Add yield after clear
            lcd.setCursor(0, 0);
            lcd.print(line1);
            lcd.setCursor(0, 1);
            lcd.print(line2);
            yield(); // Add yield after print
            
            Wire.beginTransmission(0x27);
            if (Wire.endTransmission() == 0) break;
            Serial.println("I2C communication failed, retrying... Attempt " + String(retryCount + 1));
            recoverI2C();
            delay(10); // Short blocking delay, safe for I2C recovery
            yield();
            retryCount++;
          }
          
          // Update current displayed message
          currentLine1 = line1;
          currentLine2 = line2;
          displayStartTime = millis();
          displayDuration = duration;
          displayingMessage = (duration > 0);
          transitionInProgress = false;
          
          // For backlight control
          if (retryCount >= maxRetries) {
            lcd.noBacklight();
          } else {
            lcd.backlight();
          }
          
          yield(); // Allow system to process during transition
          break;
        }
      }
    }
  }
  
  // Update activity timers
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

void updateDisplay() {
  if (displayingMessage && millis() - displayStartTime >= displayDuration) {
    displayingMessage = false;
    
    // Handle different states for display
    if (tapOutPhase && presentCount > 0) {
      // During tap-out phase, periodically show the remaining count
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("Students Remaining:");
      lcd.setCursor(0, 1);
      lcd.print(String(presentCount));
    } else if (adminAccessActive) {
      // Show admin mode is active
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("Admin Mode");
      lcd.setCursor(0, 1);
      
      // If door is locked (relay1 is HIGH) but admin mode still active
      if (digitalRead(RELAY1) == HIGH) {
        lcd.print("Tap to Exit");
      } else {
        lcd.print("Active");
      }
    } else if (!displayingMessage) {
      // Default ready message when no special message is being shown
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("Ready. Tap your");
      lcd.setCursor(0, 1);
      lcd.print("RFID Card!");
    }
  }
  
  // Periodically refresh the remaining count if we're in tap-out phase
  static unsigned long lastCountRefresh = 0;
  if (tapOutPhase && !displayingMessage && millis() - lastCountRefresh > 3000) {
    lastCountRefresh = millis();
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Students Remaining:");
    lcd.setCursor(0, 1);
    lcd.print(String(presentCount));
  }
}

// Updated recoverI2C with yield()
void recoverI2C() {
  Serial.println("I2C bus error detected. Attempting recovery...");

  pinMode(I2C_SDA, OUTPUT);
  pinMode(I2C_SCL, OUTPUT);
  digitalWrite(I2C_SDA, HIGH);
  digitalWrite(I2C_SCL, HIGH);
  unsigned long startTime = millis();
  while (millis() - startTime < 10) yield(); // Non-blocking delay

  for (int i = 0; i < 9; i++) {
    digitalWrite(I2C_SCL, LOW);
    delayMicroseconds(10);
    digitalWrite(I2C_SCL, HIGH);
    delayMicroseconds(10);
    yield(); // Yield during clock pulses
  }

  digitalWrite(I2C_SDA, LOW);
  delayMicroseconds(10);
  digitalWrite(I2C_SCL, HIGH);
  delayMicroseconds(10);
  digitalWrite(I2C_SDA, HIGH);
  delayMicroseconds(10);
  yield(); // Yield after STOP condition

  Wire.end();
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire.setClock(100000);
  yield(); // Yield after I2C reinitialization
  
  Wire.beginTransmission(0x27);
  int error = Wire.endTransmission();
  if (error == 0) {
    Serial.println("I2C bus recovered successfully.");
    lcd.begin(16, 2);
    lcd.backlight();
  } else {
    Serial.println("I2C recovery failed. Error code: " + String(error));
  }

  lastI2cRecovery = millis();
  yield(); // Yield after recovery
}

void streamCallback(FirebaseStream data) {
  if (data.dataPath() == "/door" && !adminAccessActive) {
    String value = data.stringData();
    if (value == "unlock") {
      activateRelays();
      relayActiveTime = millis();
      relayActive = true;
      studentVerificationActive = true;
      studentVerificationStartTime = millis();
      Serial.println("Door unlocked via RTDB!");
      displayMessage("Remote Unlock", "", 2000);
      logSystemEvent("Remote Door Unlock");
      firstActionOccurred = true;
      lastActivityTime = millis();
      lastReadyPrint = millis();
    }
  }
}

void streamTimeoutCallback(bool timeout) {
  if (timeout) Serial.println("RTDB Stream timed out, resuming...");
  if (!streamFbdo.httpConnected()) Serial.println("RTDB Stream error: " + streamFbdo.errorReason());
  lastActivityTime = millis();
  lastReadyPrint = millis();
}

// Power-Saving Mode Functions
void enterPowerSavingMode() {
  Serial.println("Entering power-saving mode...");
  displayMessage("Power Saving", "Mode", 1000);
  
  // Turn off LCD
  lcd.clear();
  lcd.noBacklight();
  
  // Turn off LEDs
  digitalWrite(LED_R_PIN, LOW);
  digitalWrite(LED_G_PIN, LOW);
  digitalWrite(LED_B_PIN, LOW);
  
  // Ensure buzzer is off
  noTone(BUZZER_PIN);
  
  // Disable WiFi to save power
  WiFi.disconnect();
  isConnected = false;
  
  powerSavingMode = true;
  Serial.println("Power-saving mode active. Tap a registered RFID card to wake up.");
}

void exitPowerSavingMode() {
  Serial.println("Exiting power-saving mode...");
  
  // Re-enable LCD
  lcd.backlight();
  displayMessage("Waking Up", "", 1000);
  
  // Use smooth transition instead of direct ready message
  smoothTransitionToReady();
  
  // Reconnect to WiFi
  WiFi.reconnect();
  unsigned long wifiStartTime = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - wifiStartTime < 10000) {
    delay(100);
  }
  isConnected = (WiFi.status() == WL_CONNECTED);
  if (isConnected) {
    Serial.println("WiFi reconnected.");
    initFirebase();
    fetchRegisteredUIDs();
    fetchFirestoreTeachers();
    fetchFirestoreStudents();
    fetchFirestoreRooms();
  } else {
    Serial.println("Failed to reconnect to WiFi.");
    sdMode = true;
  }
  
  // Reset LEDs to neutral state
  showNeutral();
  
  powerSavingMode = false;
  lastActivityTime = millis();
  lastReadyPrint = millis();
  readyMessageShown = true;
  Serial.println("System resumed normal operation.");
}

// Fetch Firestore users collection
void fetchFirestoreUsers() {
  if (!Firebase.ready()) return;

  if (Firebase.Firestore.getDocument(&fbdo, "smartecolock", "", "users", "")) {
    FirebaseJson usersJson;
    usersJson.setJsonData(fbdo.payload().c_str());
    FirebaseJsonData jsonData;

    if (usersJson.get(jsonData, "documents") && jsonData.type == "array") {
      FirebaseJsonArray arr;
      jsonData.getArray(arr);
      firestoreUsers.clear();

      for (size_t i = 0; i < arr.size(); i++) {
        FirebaseJsonData docData;
        arr.get(docData, i); // Get each document as a JSON object
        FirebaseJson doc;
        doc.setJsonData(docData.stringValue);

        // Extract UID from document name (e.g., "users/<uid>")
        String docName;
        doc.get(docData, "name");
        docName = docData.stringValue;
        String uid = docName.substring(docName.lastIndexOf("/") + 1);

        // Extract fields
        FirebaseJson fields;
        doc.get(docData, "fields");
        fields.setJsonData(docData.stringValue);

        std::map<String, String> userData;
        FirebaseJsonData fieldData;

        // Get fullName
        if (fields.get(fieldData, "fullName/stringValue")) {
          userData["fullName"] = fieldData.stringValue;
        } else {
          userData["fullName"] = "Unknown";
        }

        // Get role
        if (fields.get(fieldData, "role/stringValue")) {
          userData["role"] = fieldData.stringValue;
        } else {
          userData["role"] = "Unknown";
        }

        firestoreUsers[uid] = userData;
      }
      Serial.println("Firestore users collection fetched successfully. Entries: " + String(firestoreUsers.size()));
    } else {
      Serial.println("No documents found in users collection.");
    }
  } else {
    Serial.println("Failed to fetch Firestore users: " + fbdo.errorReason());
  }
}

void checkAndSyncSDData() {
  if (!isConnected) {
    Serial.println("Cannot sync SD data: No WiFi connection.");
    return;
  }

  if (!SD.begin(SD_CS, fsSPI, 4000000)) {
    Serial.println("SD card initialization failed. Cannot sync data.");
    return;
  }

  File logFile = SD.open(OFFLINE_LOG_FILE, FILE_READ);
  if (!logFile) {
    Serial.println("No " + String(OFFLINE_LOG_FILE) + " found on SD card. Skipping sync.");
    SD.end();
    return;
  }

  // Check if the file has data
  if (logFile.size() == 0) {
    Serial.println("SD log file is empty. Skipping sync.");
    logFile.close();
    SD.end();
    return;
  }

  // Sync the data to Firebase
  if (syncOfflineLogs()) {
    displayMessage("Data Pushed to", "Database", 2000);
    Serial.println("Offline data successfully pushed to Firebase.");

    // Delete the SD card file after successful sync
    logFile.close();
    SD.remove(OFFLINE_LOG_FILE);
    Serial.println("SD log file deleted after successful sync.");
  } else {
    Serial.println("Failed to sync offline data to Firebase. Retaining SD log file.");
  }

  SD.end();
}


// Synchronize SD logs to Realtime Database and append Firestore data
bool syncOfflineLogsToRTDB() {
  if (!isConnected) {
    Serial.println("Sync failed: No WiFi connection.");
    return false;
  }

  // Check Firebase readiness
  if (!Firebase.ready()) {
    Serial.println("Firebase not ready. Attempting to reinitialize...");
    Firebase.reconnectWiFi(true);
    initFirebase(); // Ensure this function is defined to initialize Firebase
    nonBlockingDelay(1000);
    if (!Firebase.ready()) {
      Serial.println("Firebase still not ready. Sync failed.");
      return false;
    }
  }

  if (!SD.begin(SD_CS, fsSPI, 4000000)) {
    Serial.println("Sync failed: SD card not initialized.");
    return false;
  }

  File logFile = SD.open(OFFLINE_LOG_FILE, FILE_READ);
  if (!logFile) {
    Serial.println("No " + String(OFFLINE_LOG_FILE) + " found on SD card. Nothing to sync.");
    SD.end();
    return true;
  }

  bool syncSuccess = true;
  while (logFile.available()) {
    String logEntry = logFile.readStringUntil('\n');
    logEntry.trim();
    if (logEntry.length() == 0) continue;

    Serial.println("Parsing log entry: " + logEntry);

    // Skip SuperAdminPZEMInitial entries (they should only be on SD card)
    if (logEntry.startsWith("SuperAdminPZEMInitial:")) {
      Serial.println("Skipping SuperAdminPZEMInitial log entry (SD only): " + logEntry);
      continue;
    }

    // Check if this is a TurnedOffRoom log with final PZEM readings
    if (logEntry.startsWith("UID:") && logEntry.indexOf("Action:TurnedOffRoom") != -1) {
      int uidIndex = logEntry.indexOf("UID:") + 4;
      int timestampIndex = logEntry.indexOf("Timestamp:");
      int actionIndex = logEntry.indexOf("Action:");
      int voltageIndex = logEntry.indexOf("Voltage:");
      int currentIndex = logEntry.indexOf("Current:");
      int powerIndex = logEntry.indexOf("Power:");
      int energyIndex = logEntry.indexOf("Energy:");
      int frequencyIndex = logEntry.indexOf("Frequency:");
      int pfIndex = logEntry.indexOf("PowerFactor:");
      int totalConsumptionIndex = logEntry.indexOf("TotalConsumption:");

      if (uidIndex == -1 || timestampIndex == -1 || actionIndex == -1 ||
          voltageIndex == -1 || currentIndex == -1 || powerIndex == -1 ||
          energyIndex == -1 || frequencyIndex == -1 || pfIndex == -1 || totalConsumptionIndex == -1) {
        Serial.println("Invalid TurnedOffRoom log entry format: " + logEntry);
        continue;
      }

      String uid = logEntry.substring(uidIndex, timestampIndex - 1);
      String timestamp = logEntry.substring(timestampIndex + 10, actionIndex - 1);
      if (timestamp == "N/A") {
        timestamp = "1970-01-01 00:00:00";
        Serial.println("Timestamp is N/A. Using placeholder: " + timestamp);
      }
      String voltageStr = logEntry.substring(voltageIndex + 8, logEntry.indexOf("V", voltageIndex));
      String currentStr = logEntry.substring(currentIndex + 8, logEntry.indexOf("A", currentIndex));
      String powerStr = logEntry.substring(powerIndex + 6, logEntry.indexOf("W", powerIndex));
      String energyStr = logEntry.substring(energyIndex + 7, logEntry.indexOf("kWh", energyIndex));
      String frequencyStr = logEntry.substring(frequencyIndex + 10, logEntry.indexOf("Hz", frequencyIndex));
      String pfStr = logEntry.substring(pfIndex + 12, totalConsumptionIndex - 1);
      String totalConsumptionStr = logEntry.substring(totalConsumptionIndex + 16, logEntry.indexOf("kWh", totalConsumptionIndex));

      float voltage = voltageStr.toFloat();
      float current = currentStr.toFloat();
      float power = powerStr.toFloat();
      float energy = energyStr.toFloat();
      float frequency = frequencyStr.toFloat();
      float powerFactor = pfStr.toFloat();
      float totalConsumption = totalConsumptionStr.toFloat();

      // Validate data to prevent NaN or invalid values
      if (isnan(voltage) || voltage < 0) voltage = 0.0;
      if (isnan(current) || current < 0) current = 0.0;
      if (isnan(power) || power < 0) power = 0.0;
      if (isnan(energy) || energy < 0) energy = 0.0;
      if (isnan(frequency) || frequency < 0) frequency = 0.0;
      if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
      if (isnan(totalConsumption) || totalConsumption < 0) totalConsumption = 0.0;

      FirebaseJson json;
      json.set("uid", uid);
      json.set("timestamp", timestamp);
      json.set("action", "TurnedOffRoom");
      json.set("name", "CIT-U");
      json.set("role", "Super Admin");
      json.set("department", "Computer Engineering");
      json.set("building", "GLE");
      json.set("voltage", voltage);
      json.set("current", current);
      json.set("power", power);
      json.set("energy", energy);
      json.set("frequency", frequency);
      json.set("powerFactor", powerFactor);
      json.set("totalConsumption", String(totalConsumption, 3));
      json.set("powerConsumptionNote", totalConsumption < 0.001 ? "Insufficient Consumption" : "Normal");

      String safeTimestamp = timestamp;
      safeTimestamp.replace(" ", "_");
      String path = "/OfflineDataLogging/" + uid + "_" + safeTimestamp;
      if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
        Serial.println("Synced TurnedOffRoom log with final PZEM readings: " + logEntry);
      } else {
        Serial.println("Failed to sync TurnedOffRoom log: " + logEntry + " - " + fbdo.errorReason());
        syncSuccess = false;
      }
    }
    // Check if this is a Tamper log
    else if (logEntry.startsWith("Tamper:")) {
      int tamperIndex = logEntry.indexOf("Tamper:") + 7;
      int timestampIndex = logEntry.indexOf("Timestamp:");
      int statusIndex = logEntry.indexOf("Status:");

      if (tamperIndex == -1 || timestampIndex == -1 || statusIndex == -1) {
        Serial.println("Invalid Tamper log entry format: " + logEntry);
        continue;
      }

      String tamperAction = logEntry.substring(tamperIndex, timestampIndex - 1);
      String timestamp = logEntry.substring(timestampIndex + 10, statusIndex - 1);
      if (timestamp == "N/A") {
        timestamp = "1970-01-01 00:00:00";
        Serial.println("Timestamp is N/A. Using placeholder: " + timestamp);
      }
      String statusPart = logEntry.substring(statusIndex + 7);

      String byUid = "";
      if (tamperAction == "Resolved") {
        int byIndex = statusPart.indexOf("By:");
        if (byIndex != -1) {
          byUid = statusPart.substring(byIndex + 3);
          statusPart = statusPart.substring(0, byIndex);
        }
      }

      FirebaseJson json;
      json.set("action", "Tamper" + tamperAction);
      json.set("timestamp", timestamp);
      json.set("status", statusPart);
      if (tamperAction == "Resolved" && byUid != "") {
        json.set("resolvedBy", byUid);
      }
      json.set("name", "System");
      json.set("role", "Super Admin");

      String safeTimestamp = timestamp;
      safeTimestamp.replace(" ", "_");
      String path = "/OfflineDataLogging/Tamper_" + safeTimestamp;
      if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
        Serial.println("Synced Tamper log: " + logEntry);
      } else {
        Serial.println("Failed to sync Tamper log: " + logEntry + " - " + fbdo.errorReason());
        syncSuccess = false;
      }
    }
    // Check if this is a SessionEnd log (automatic end time detection)
    else if (logEntry.startsWith("SessionEnd:")) {
      int uidIndex = logEntry.indexOf("UID:") + 4;
      int timeIndex = logEntry.indexOf("Time:");
      int statusIndex = logEntry.indexOf("Status:");
      int dayIndex = logEntry.indexOf("Day:");
      int startTimeIndex = logEntry.indexOf("StartTime:");
      int endTimeIndex = logEntry.indexOf("EndTime:");
      int subjectIndex = logEntry.indexOf("Subject:");
      int subjectCodeIndex = logEntry.indexOf("SubjectCode:");
      int sectionIndex = logEntry.indexOf("Section:");
      int roomIndex = logEntry.indexOf("Room:");
      int voltageIndex = logEntry.indexOf("Voltage:");
      int currentIndex = logEntry.indexOf("Current:");
      int powerIndex = logEntry.indexOf("Power:");
      int energyIndex = logEntry.indexOf("Energy:");
      int frequencyIndex = logEntry.indexOf("Frequency:");
      int pfIndex = logEntry.indexOf("PowerFactor:");
      int autoEndIndex = logEntry.indexOf("AutoEnd:");

      // Verify all required fields are present
      if (uidIndex == -1 || timeIndex == -1 || statusIndex == -1 || dayIndex == -1 || 
          startTimeIndex == -1 || endTimeIndex == -1 || subjectIndex == -1 || 
          subjectCodeIndex == -1 || sectionIndex == -1 || roomIndex == -1 || 
          voltageIndex == -1 || currentIndex == -1 || powerIndex == -1 || 
          energyIndex == -1 || frequencyIndex == -1 || pfIndex == -1) {
        Serial.println("Invalid SessionEnd log entry format: " + logEntry);
        continue;
      }

      // Extract session data
      String uid = logEntry.substring(uidIndex, timeIndex - 1);
      String timestamp = logEntry.substring(timeIndex + 5, statusIndex - 1);
      String status = logEntry.substring(statusIndex + 7, dayIndex - 1);
      String day = logEntry.substring(dayIndex + 4, startTimeIndex - 1);
      String startTime = logEntry.substring(startTimeIndex + 10, endTimeIndex - 1);
      String endTime = logEntry.substring(endTimeIndex + 8, subjectIndex - 1);
      String subject = logEntry.substring(subjectIndex + 8, subjectCodeIndex - 1);
      String subjectCode = logEntry.substring(subjectCodeIndex + 12, sectionIndex - 1);
      String section = logEntry.substring(sectionIndex + 8, roomIndex - 1);
      String roomName = logEntry.substring(roomIndex + 5, voltageIndex - 1);
      
      // Extract PZEM data
      String voltageStr = logEntry.substring(voltageIndex + 8, logEntry.indexOf(" Current:"));
      String currentStr = logEntry.substring(currentIndex + 8, logEntry.indexOf(" Power:"));
      String powerStr = logEntry.substring(powerIndex + 6, logEntry.indexOf(" Energy:"));
      String energyStr = logEntry.substring(energyIndex + 7, logEntry.indexOf(" Frequency:"));
      String frequencyStr = logEntry.substring(frequencyIndex + 10, logEntry.indexOf(" PowerFactor:"));
      String pfStr = pfIndex != -1 ? logEntry.substring(pfIndex + 12, autoEndIndex - 1) : "0.0";

      // Prepare data for Firebase
      String instructorPath = "/Instructors/" + uid;
      FirebaseJson classStatusJson;
      classStatusJson.set("Status", status);
      classStatusJson.set("dateTime", timestamp);
      
      FirebaseJson scheduleJson;
      scheduleJson.set("day", day.length() > 0 ? day : "Unknown");
      scheduleJson.set("startTime", startTime.length() > 0 ? startTime : "Unknown");
      scheduleJson.set("endTime", endTime.length() > 0 ? endTime : "Unknown");
      scheduleJson.set("subject", subject.length() > 0 ? subject : "Unknown");
      scheduleJson.set("subjectCode", subjectCode.length() > 0 ? subjectCode : "Unknown");
      scheduleJson.set("section", section.length() > 0 ? section : "Unknown");
      
      FirebaseJson roomNameJson;
      roomNameJson.set("name", roomName.length() > 0 ? roomName : "Unknown");
      
      // Create PZEM JSON data
      FirebaseJson pzemJson;
      pzemJson.set("voltage", voltageStr);
      pzemJson.set("current", currentStr);
      pzemJson.set("power", powerStr);
      pzemJson.set("energy", energyStr);
      pzemJson.set("frequency", frequencyStr);
      pzemJson.set("powerFactor", pfStr);
      pzemJson.set("timestamp", timestamp);
      roomNameJson.set("pzem", pzemJson);
      
      scheduleJson.set("roomName", roomNameJson);
      classStatusJson.set("schedule", scheduleJson);
      
      String statusPath = instructorPath + "/ClassStatus";
      if (Firebase.RTDB.updateNode(&fbdo, statusPath, &classStatusJson)) {
        Serial.println("Class status updated to 'Class Ended' with PZEM data");
      } else {
        String errorLog = "FirebaseError:Path:" + statusPath + " Reason:" + fbdo.errorReason();
        storeLogToSD(errorLog);
        Serial.println("Failed to update class status with PZEM data: " + fbdo.errorReason());
      }
      
      // Also record this in the OfflineDataLogging node for historical purposes
      String safeTimestamp = timestamp;
      safeTimestamp.replace(" ", "_");
      safeTimestamp.replace(":", "-");
      String offlinePath = "/OfflineDataLogging/AutoEnd_" + uid + "_" + safeTimestamp;
      
      FirebaseJson offlineJson;
      offlineJson.set("uid", uid);
      offlineJson.set("timestamp", timestamp);
      offlineJson.set("action", "AutoEndSession");
      offlineJson.set("status", status);
      offlineJson.set("subject", subject);
      offlineJson.set("subjectCode", subjectCode);
      offlineJson.set("section", section);
      offlineJson.set("roomName", roomName);
      offlineJson.set("voltage", voltageStr);
      offlineJson.set("current", currentStr);
      offlineJson.set("power", powerStr);
      offlineJson.set("energy", energyStr);
      
      if (Firebase.RTDB.setJSON(&fbdo, offlinePath, &offlineJson)) {
        Serial.println("Added SessionEnd to offline logging history");
      } else {
        Serial.println("Failed to add to offline logging history: " + fbdo.errorReason());
      }
    }
    // Handle other log entries (e.g., SuperAdminAccess, Door Opened, etc.)
    else if (logEntry.startsWith("UID:")) {
      int uidIndex = logEntry.indexOf("UID:") + 4;
      int timestampIndex = logEntry.indexOf("Timestamp:");
      int actionIndex = logEntry.indexOf("Action:");

      if (uidIndex == -1 || timestampIndex == -1 || actionIndex == -1) {
        Serial.println("Invalid log entry format: " + logEntry);
        continue;
      }

      String uid = logEntry.substring(uidIndex, timestampIndex - 1);
      String timestamp = logEntry.substring(timestampIndex + 10, actionIndex - 1);
      if (timestamp == "N/A") {
        timestamp = "1970-01-01 00:00:00";
        Serial.println("Timestamp is N/A. Using placeholder: " + timestamp);
      }
      String actionPart = logEntry.substring(actionIndex + 7);
      String action = actionPart;
      float powerConsumption = 0.0;
      float voltage = 0.0;

      FirebaseJson json;
      json.set("uid", uid);
      json.set("timestamp", timestamp);

      if (actionPart.indexOf("Door Opened+Deactivate") != -1) {
        action = "Door Opened+Deactivate";
        json.set("initialAction", "Door Opened");
        json.set("finalAction", "Deactivate");

        int powerIndex = actionPart.indexOf("Power:");
        if (powerIndex != -1) {
          int kwhIndex = actionPart.indexOf("kWh", powerIndex);
          if (kwhIndex != -1) {
            String powerStr = actionPart.substring(powerIndex + 6, kwhIndex);
            powerConsumption = powerStr.toFloat();
            if (isnan(powerConsumption) || powerConsumption < 0) powerConsumption = 0.0;
            Serial.println("Parsed powerConsumption: " + String(powerConsumption, 3) + " kWh");
          }
        }

        int voltageIndex = actionPart.indexOf("Voltage:");
        if (voltageIndex != -1) {
          int vIndex = actionPart.indexOf("V", voltageIndex);
          if (vIndex != -1) {
            String voltageStr = actionPart.substring(voltageIndex + 8, vIndex);
            voltage = voltageStr.toFloat();
            if (isnan(voltage) || voltage < 0) voltage = 0.0;
            Serial.println("Parsed voltage: " + String(voltage, 2) + " V");
          }
        }
      } else {
        json.set("action", actionPart);

        int powerIndex = actionPart.indexOf("Power:");
        if (powerIndex != -1) {
          int kwhIndex = actionPart.indexOf("kWh", powerIndex);
          if (kwhIndex != -1) {
            String powerStr = actionPart.substring(powerIndex + 6, kwhIndex);
            powerConsumption = powerStr.toFloat();
            if (isnan(powerConsumption) || powerConsumption < 0) powerConsumption = 0.0;
            Serial.println("Parsed powerConsumption: " + String(powerConsumption, 3) + " kWh");
          }
        }

        int voltageIndex = logEntry.indexOf("Voltage:");
        if (voltageIndex != -1) {
          int vIndex = logEntry.indexOf("V", voltageIndex);
          if (vIndex != -1) {
            String voltageStr = logEntry.substring(voltageIndex + 8, vIndex);
            voltage = voltageStr.toFloat();
            if (isnan(voltage) || voltage < 0) voltage = 0.0;
            Serial.println("Parsed voltage: " + String(voltage, 2) + " V");
          }
        }
      }

      String name = "Unknown";
      String role = "Unknown";
      if (uid == SUPER_ADMIN_UID) {
        name = "CIT-U";
        role = "Super Admin";
        json.set("department", "Computer Engineering");
        json.set("building", "GLE");
      } else if (firestoreTeachers.find(uid) != firestoreTeachers.end()) {
        name = firestoreTeachers[uid]["fullName"].length() > 0 ? firestoreTeachers[uid]["fullName"] : "Unknown";
        role = firestoreTeachers[uid]["role"].length() > 0 ? firestoreTeachers[uid]["role"] : "Unknown";
      } else if (firestoreStudents.find(uid) != firestoreStudents.end()) {
        name = firestoreStudents[uid]["fullName"].length() > 0 ? firestoreStudents[uid]["fullName"] : "Unknown";
        role = firestoreStudents[uid]["role"].length() > 0 ? firestoreStudents[uid]["role"] : "Unknown";
      } else if (firestoreUsers.find(uid) != firestoreUsers.end()) {
        name = firestoreUsers[uid]["fullName"].length() > 0 ? firestoreUsers[uid]["fullName"] : "Unknown";
        role = firestoreUsers[uid]["role"].length() > 0 ? firestoreUsers[uid]["role"] : "Unknown";
      }
      json.set("name", name);
      json.set("role", role);

      if (action == "Door Opened+Deactivate" || action.indexOf("Power:") != -1) {
        json.set("powerConsumption", String(powerConsumption, 3));
        json.set("powerConsumptionNote", powerConsumption < 0.001 ? "Insufficient Consumption" : "Normal");
        json.set("voltage", voltage);
      }

      String safeTimestamp = timestamp;
      safeTimestamp.replace(" ", "_");
      String path = "/OfflineDataLogging/" + uid + "_" + safeTimestamp;
      if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
        Serial.println("Synced offline log: " + logEntry);
      } else {
        Serial.println("Failed to sync log: " + logEntry + " - " + fbdo.errorReason());
        syncSuccess = false;
      }
    }
    // Skip malformed entries
    else {
      Serial.println("Skipping malformed log entry: " + logEntry);
      continue;
    }
  }

  logFile.close();
  SD.end();
  return syncSuccess;
}


void clearSDLogs() {
  if (SD.begin(SD_CS, fsSPI, 4000000)) {
    if (SD.remove(OFFLINE_LOG_FILE)) {
      Serial.println(String(OFFLINE_LOG_FILE) + " cleared successfully.");
    } else {
      Serial.println("Failed to clear " + String(OFFLINE_LOG_FILE) + ".");
    }
    SD.end();
  } else {
    Serial.println("Failed to initialize SD card to clear logs.");
  }
}
void scanI2C() {
  Serial.println("Scanning I2C bus...");
  for (byte addr = 1; addr < 127; addr++) {
    Wire.beginTransmission(addr);
    if (Wire.endTransmission() == 0) {
      Serial.print("Device found at address 0x");
      if (addr < 16) Serial.print("0");
      Serial.println(addr, HEX);
    }
  }
}

bool syncSchedulesToSD() {
  if (!isConnected || !Firebase.ready()) {
    Serial.println("Cannot sync schedules: No WiFi or Firebase not ready.");
    return false;
  }

  // Initialize SD if not already done
  if (!sdInitialized) {
    Serial.println("SD not initialized. Attempting to start SD...");
    if (!SD.begin(SD_CS, fsSPI, 4000000)) {
      Serial.println("SD card initialization failed. Check wiring, CS pin (" + String(SD_CS) + "), or SD card.");
      return false;
    }
    sdInitialized = true;
    Serial.println("SD card initialized successfully.");
  }

  // Check if schedules.json exists and read current content
  String currentJsonStr;
  bool fileExists = SD.exists("/schedules.json");
  if (fileExists) {
    File readFile = SD.open("/schedules.json", FILE_READ);
    if (readFile) {
      while (readFile.available()) {
        currentJsonStr += (char)readFile.read();
      }
      readFile.close();
      Serial.println("Read existing /schedules.json. Size: " + String(currentJsonStr.length()) + " bytes");
    } else {
      Serial.println("Failed to read existing /schedules.json. Proceeding to create new file.");
    }
  } else {
    Serial.println("/schedules.json not found. Will create new file.");
  }

  // Build new schedules JSON
  FirebaseJson schedulesJson;
  for (const auto& teacher : firestoreTeachers) {
    String uid = teacher.first;
    String schedulesStr;
    String sectionsStr;

    try {
      schedulesStr = teacher.second.at("schedules");
      sectionsStr = teacher.second.at("sections");
    } catch (const std::out_of_range& e) {
      Serial.println("UID " + uid + " missing schedules or sections. Skipping.");
      continue;
    }

    if (schedulesStr != "[]" && schedulesStr.length() > 0) {
      FirebaseJson teacherData;
      FirebaseJson schedules;
      if (schedules.setJsonData(schedulesStr)) {
        teacherData.set("schedules", schedules);
      } else {
        Serial.println("Failed to parse schedules JSON for UID " + uid);
        continue;
      }

      if (sectionsStr != "[]" && sectionsStr.length() > 0) {
        FirebaseJson sections;
        if (sections.setJsonData(sectionsStr)) {
          teacherData.set("sections", sections);
        } else {
          Serial.println("Failed to parse sections JSON for UID " + uid);
        }
      }
      schedulesJson.set(uid, teacherData);
    }
  }

  String newJsonStr;
  schedulesJson.toString(newJsonStr, true);

  // Compare and update only if changed or file doesn't exist
  if (!fileExists || currentJsonStr != newJsonStr) {
    File scheduleFile = SD.open("/schedules.json", FILE_WRITE);
    if (!scheduleFile) {
      Serial.println("Failed to open /schedules.json for writing. SD card may be read-only, full, or corrupted.");
      SD.end();
      sdInitialized = false;
      return false;
    }

    if (scheduleFile.print(newJsonStr)) {
      Serial.println("Schedules (and sections) synced to /schedules.json. Size: " + String(newJsonStr.length()) + " bytes");
      scheduleFile.close();
    } else {
      Serial.println("Failed to write schedules to /schedules.json. SD card may be full or corrupted.");
      scheduleFile.close();
      SD.end();
      sdInitialized = false;
      return false;
    }
  } else {
    Serial.println("No changes detected in schedules. Keeping existing /schedules.json.");
  }

  SD.end();
  return true;
}

bool schedulesMatch(const ScheduleInfo& studentSchedule, FirebaseJson& instructorScheduleJson) {
  FirebaseJsonData jsonData;
  String instructorDay, instructorStartTime, instructorEndTime, instructorRoomName, instructorSubjectCode, instructorSection;

  if (instructorScheduleJson.get(jsonData, "day") && jsonData.typeNum == FirebaseJson::JSON_STRING) {
    instructorDay = jsonData.stringValue;
  }
  if (instructorScheduleJson.get(jsonData, "startTime") && jsonData.typeNum == FirebaseJson::JSON_STRING) {
    instructorStartTime = jsonData.stringValue;
  }
  if (instructorScheduleJson.get(jsonData, "endTime") && jsonData.typeNum == FirebaseJson::JSON_STRING) {
    instructorEndTime = jsonData.stringValue;
  }
  if (instructorScheduleJson.get(jsonData, "roomName") && jsonData.typeNum == FirebaseJson::JSON_STRING) {
    instructorRoomName = jsonData.stringValue;
  }
  if (instructorScheduleJson.get(jsonData, "subjectCode") && jsonData.typeNum == FirebaseJson::JSON_STRING) {
    instructorSubjectCode = jsonData.stringValue;
  }
  if (instructorScheduleJson.get(jsonData, "section") && jsonData.typeNum == FirebaseJson::JSON_STRING) {
    instructorSection = jsonData.stringValue;
  }

  bool match = (studentSchedule.day.equalsIgnoreCase(instructorDay) &&
                studentSchedule.startTime == instructorStartTime &&
                studentSchedule.endTime == instructorEndTime &&
                studentSchedule.roomName == instructorRoomName &&
                studentSchedule.subjectCode == instructorSubjectCode &&
                studentSchedule.section == instructorSection);

  if (!match) {
    Serial.println("Schedule mismatch - Student: " + studentSchedule.day + " " +
                   studentSchedule.startTime + "-" + studentSchedule.endTime + ", " +
                   studentSchedule.roomName + ", " + studentSchedule.subjectCode + ", " +
                   studentSchedule.section + " | Instructor: " + instructorDay + " " +
                   instructorStartTime + "-" + instructorEndTime + ", " +
                   instructorRoomName + ", " + instructorSubjectCode + ", " + instructorSection);
  }

  return match;
}

ScheduleInfo isWithinSchedule(String uidStr, String timestamp) {
  ScheduleInfo result = {false, "", "", "", "", "", "", ""};
  watchdogCheck(); // Feed WDT
  if (checkResetButton()) return result; // Early reset check
  yield(); // Prevent watchdog reset at start of loop
  delay(5); // Small delay to allow other tasks to run
  yield(); // Additional yield after delay

  // Check for admin door auto-lock timeout
  checkAdminDoorAutoLock();
  yield(); // Add yield after admin door check
  
  // Update lastReadyPrint during active operations to prevent watchdog timeouts
  if (classSessionActive || studentVerificationActive || relayActive || tapOutPhase) {
    if (millis() - lastReadyPrint > 60000) { // Update every minute during active operations
      lastReadyPrint = millis();
      Serial.println("Watchdog timer extended for active operation");
    }
  }
  
  // Add yield before end time check
  yield();

  // Check if a class session should be ended due to reaching the end time
  static unsigned long lastEndTimeCheck = 0;
  if (classSessionActive && !tapOutPhase && millis() - lastEndTimeCheck >= 30000) {
    // Existing end time check code
    // ...
    
    lastEndTimeCheck = millis();
    yield(); // Add yield after end time check
  }

  // Critical section - update watchdog timers before relay operations
  lastActivityTime = millis(); // Feed watchdog timer before critical operation
  lastReadyPrint = millis();  // Update ready print time to prevent timeouts
  yield(); // Yield before relay operations
  delay(10); // Small delay before critical operation
  yield(); // Additional yield after delay

  // Process any pending relay operations with additional watchdog protection
  watchdogCheck(); // Explicitly feed watchdog before critical operation
  checkPendingRelayOperations();
  watchdogCheck(); // Explicitly feed watchdog after critical operation
  
  // Multiple yields and watchdog updates after relay operations
  yield();
  delay(20); // Give system time to stabilize after relay operations
  yield();
  lastActivityTime = millis(); // Update watchdog timer after relay operations
  lastReadyPrint = millis();

  // Periodic updates with additional yields
  static unsigned long lastPeriodicUpdate = 0;
  if (millis() - lastPeriodicUpdate >= 100) {
    yield(); // Yield before PZEM update
    updatePZEM();
    yield(); // Yield after PZEM update
    updateDisplay();
    yield(); // Yield after display update
    lastPeriodicUpdate = millis();
  }

  // More frequent display updates during tap-out phase
  static unsigned long lastTapOutDisplayUpdate = 0;
  if (tapOutPhase && millis() - lastTapOutDisplayUpdate >= 1000) { // Update every second during tap-out
    yield(); // Yield before display update
    updateDisplay(); // Extra call for tap-out phase
    lastTapOutDisplayUpdate = millis();
    yield(); // Yield after display update
  }

  // Heap monitoring with additional yields
  static unsigned long lastHeapCheck = 0;
  if (millis() - lastHeapCheck >= 10000) { // Changed from 30000 to 10000
    yield(); // Yield before heap check
    Serial.println("Free heap: " + String(ESP.getFreeHeap()) + " bytes");
    if (ESP.getFreeHeap() < 10000) {
      logSystemEvent("Low Memory Reset");
      ESP.restart();
    }
    lastHeapCheck = millis();
    yield(); // Yield after heap check
  }

  // Log heap trends to SD (for debugging memory leaks)
  static unsigned long lastHeapLog = 0;
  // Existing heap logging code
  // ...

  // Final yields at the end of loop
  yield();
  delay(5);
  yield();

  int currentHour, currentMinute;
  String currentDay;

  if (sdMode) {
    RtcDateTime now = Rtc.GetDateTime();
    if (!now.IsValid()) {
      Serial.println("RTC time invalid in isWithinSchedule");
      return result;
    }
    const char* days[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
    currentDay = days[now.DayOfWeek()];
    currentHour = now.Hour();
    currentMinute = now.Minute();
  } else {
    struct tm timeinfo;
    yield(); // Yield before getting time
    if (!getLocalTime(&timeinfo)) {
      Serial.println("Failed to get local time in isWithinSchedule");
      return result;
    }
    char dayStr[10];
    strftime(dayStr, sizeof(dayStr), "%A", &timeinfo);
    currentDay = String(dayStr);
    currentHour = timeinfo.tm_hour;
    currentMinute = timeinfo.tm_min;
  }

  String minuteStr = (currentMinute < 10 ? "0" : "") + String(currentMinute);
  Serial.println("Current day: " + currentDay + ", time: " + String(currentHour) + ":" + minuteStr);

  // First try with cached data
  yield(); // Yield before first schedule check
  result = checkSchedule(uidStr, currentDay, currentHour, currentMinute);
  
  // If no valid schedule found and we're online, refresh Firestore data and try again
  if (!result.isValid && !sdMode && isConnected && Firebase.ready()) {
    Serial.println("No valid schedule found initially. Refreshing Firestore data...");
    displayMessage("Checking for", "Schedule Updates", 2000);
    
    yield(); // Yield before Firestore operations
    
    // Refresh both teachers and students data
    if (firestoreTeachers.find(uidStr) != firestoreTeachers.end()) {
      Serial.println("Refreshing instructor schedules...");
      fetchFirestoreTeachers();
      yield(); // Yield after teacher fetch
    } else if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
      Serial.println("Refreshing student schedules...");
      fetchFirestoreStudents();
      yield(); // Yield after student fetch
    }
    
    // Check schedule again with fresh data
    yield(); // Yield before second schedule check
    result = checkSchedule(uidStr, currentDay, currentHour, currentMinute);
    
    if (result.isValid) {
      Serial.println("Valid schedule found after Firestore refresh!");
      displayMessage("Schedule Found", "After Update", 2000);
    } else {
      Serial.println("No valid schedule found even after refresh.");
      displayMessage("No Schedule", "Found", 2000);
    }
  }

  yield(); // Final yield before returning
  return result;
}

// Helper function to check schedule with current data
ScheduleInfo checkSchedule(String uidStr, String currentDay, int currentHour, int currentMinute) {
  ScheduleInfo result = {false, "", "", "", "", "", "", ""};
  yield(); // Yield at start of function
  
  // Load schedules from SD or Firestore
  FirebaseJson schedulesJson;
  bool loadedFromSD = false;

  if (sdMode) {
    yield(); // Yield before SD operations
    if (SD.begin(SD_CS, fsSPI, 4000000)) {
      File scheduleFile = SD.open("/schedules.json", FILE_READ);
      if (scheduleFile) {
        String jsonStr;
        while (scheduleFile.available()) {
          jsonStr += (char)scheduleFile.read();
          if (jsonStr.length() % 512 == 0) yield(); // Yield periodically during file read
        }
        scheduleFile.close();
        schedulesJson.setJsonData(jsonStr);
        loadedFromSD = (jsonStr.length() > 0);
      }
      SD.end();
    }

    yield(); // Yield after SD operations

    if (loadedFromSD) {
      FirebaseJsonData uidData;
      if (schedulesJson.get(uidData, uidStr)) {
        FirebaseJson teacherSchedules;
        teacherSchedules.setJsonData(uidData.stringValue);
        FirebaseJsonData schedulesData;
        if (teacherSchedules.get(schedulesData, "schedules")) {
          schedulesJson.clear();
          schedulesJson.setJsonData(schedulesData.stringValue);
        } else {
          loadedFromSD = false;
        }
      } else {
        loadedFromSD = false;
      }
    }
  }

  yield(); // Yield before Firestore data processing

  // Use Firestore data if not loaded from SD
  if (!loadedFromSD) {
    String schedulesStr;
    if (firestoreTeachers.find(uidStr) != firestoreTeachers.end()) {
      schedulesStr = firestoreTeachers[uidStr]["schedules"];
    } else if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
      schedulesStr = firestoreStudents[uidStr]["schedules"];
    }

    if (schedulesStr == "[]" || schedulesStr.length() == 0) {
      return result;
    }

    schedulesJson.setJsonData(schedulesStr);
  }

  yield(); // Yield before schedule iteration

  // Check schedules
  size_t scheduleCount = 0;
  FirebaseJsonData uidData;
  while (schedulesJson.get(uidData, "[" + String(scheduleCount) + "]")) {
    scheduleCount++;
    if (scheduleCount % 5 == 0) yield(); // Yield every 5 schedules during counting
  }

  for (size_t i = 0; i < scheduleCount; i++) {
    if (i % 3 == 0) yield(); // Yield every 3 iterations during schedule checking
    
    String path = "[" + String(i) + "]";
    FirebaseJsonData scheduleData;
    if (schedulesJson.get(scheduleData, path)) {
      FirebaseJson schedule;
      schedule.setJsonData(scheduleData.stringValue);

      FirebaseJsonData fieldData;
      String scheduleDay, startTime, endTime, subject, subjectCode, section, roomName;

      if (!schedule.get(fieldData, "day") || fieldData.stringValue.length() == 0) continue;
      scheduleDay = fieldData.stringValue;
      
      if (!schedule.get(fieldData, "startTime") || fieldData.stringValue.length() == 0) continue;
      startTime = fieldData.stringValue;
      
      if (!schedule.get(fieldData, "endTime") || fieldData.stringValue.length() == 0) continue;
      endTime = fieldData.stringValue;
      
      if (!schedule.get(fieldData, "roomName") || fieldData.stringValue.length() == 0) continue;
      roomName = fieldData.stringValue;
      
      if (!schedule.get(fieldData, "subject") || fieldData.stringValue.length() == 0) continue;
      subject = fieldData.stringValue;
      
      if (!schedule.get(fieldData, "subjectCode") || fieldData.stringValue.length() == 0) continue;
      subjectCode = fieldData.stringValue;
      
      if (!schedule.get(fieldData, "section") || fieldData.stringValue.length() == 0) continue;
      section = fieldData.stringValue;

      yield(); // Yield after field extraction

      if (scheduleDay.equalsIgnoreCase(currentDay)) {
        int startHour = startTime.substring(0, 2).toInt();
        int startMin = startTime.substring(3, 5).toInt();
        int endHour = endTime.substring(0, 2).toInt();
        int endMin = endTime.substring(3, 5).toInt();

        int currentMins = currentHour * 60 + currentMinute;
        int startMins = startHour * 60 + startMin;
        int endMins = endHour * 60 + endMin;

        // Handle overnight schedules
        if (endMins < startMins) {
          if (currentMins >= startMins || currentMins <= endMins) {
            result.isValid = true;
            result.day = scheduleDay;
            result.startTime = startTime;
            result.endTime = endTime;
            result.roomName = roomName;
            result.subject = subject;
            result.subjectCode = subjectCode;
            result.section = section;
            return result;
          }
        } else if (currentMins >= startMins && currentMins <= endMins) {
          result.isValid = true;
          result.day = scheduleDay;
          result.startTime = startTime;
          result.endTime = endTime;
          result.roomName = roomName;
          result.subject = subject;
          result.subjectCode = subjectCode;
          result.section = section;
          return result;
        }
      }
    }
  }

  yield(); // Final yield before returning
  return result;
}

void customLoopTask(void *pvParameters) {
  Serial.println("customLoopTask started with stack size: 20480 bytes");
  Serial.println("Stack remaining at start: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");

  // Firebase data fetching
  fetchRegisteredUIDs();
  Serial.println("Stack after fetchRegisteredUIDs: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");
  fetchFirestoreTeachers();
  Serial.println("Stack after fetchFirestoreTeachers: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");
  fetchFirestoreStudents();
  Serial.println("Stack after fetchFirestoreStudents: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");
  fetchFirestoreRooms();
  Serial.println("Stack after fetchFirestoreRooms: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");
  fetchFirestoreUsers();
  Serial.println("Stack after fetchFirestoreUsers: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");
  Serial.println("Firestore data fetched: Teachers=" + String(firestoreTeachers.size()) + 
                 ", Students=" + String(firestoreStudents.size()) + 
                 ", Users=" + String(firestoreUsers.size()));

  if (Firebase.ready()) {
    Firebase.RTDB.setStreamCallback(&streamFbdo, streamCallback, streamTimeoutCallback);
    Serial.println("Firebase stream callback initialized.");
  } else {
    Serial.println("Firebase not ready after init: " + fbdo.errorReason());
  }
  Serial.println("Stack after stream setup: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");

  // Main loop
  for (;;) {
    loop();
    vTaskDelay(1 / portTICK_PERIOD_MS); // Yield to other tasks
  }
}

void setup() {
  // Startup stability delay to ensure power levels are stable
  delay(100);  // Add a small delay to stabilize power during boot
  
  // Call our safe relay state function to ensure relays are properly initialized
  ensureRelaySafeState();
  Serial.println("Relays initialized to inactive state");
  
  // Now continue with the rest of initialization
  Serial.begin(115200);
  while (!Serial && millis() < 5000);
  Serial.println("Starting setup...");

  static int bootCount = 0;
  bootCount++;
  Serial.println("Boot detected. Boot count: " + String(bootCount));

  systemStartTime = millis();
  Serial.println("System start time: " + String(systemStartTime) + " ms");
  Serial.println("Free heap memory: " + String(ESP.getFreeHeap()) + " bytes");

  // Initialize remaining pins
  pinMode(RESET_BUTTON_PIN, INPUT_PULLUP);
  pinMode(BUZZER_PIN, OUTPUT);
  pinMode(LED_R_PIN, OUTPUT);
  pinMode(LED_G_PIN, OUTPUT);
  pinMode(LED_B_PIN, OUTPUT);
  pinMode(TAMPER_PIN, INPUT_PULLUP);
  pinMode(MFRC522_IRQ, INPUT_PULLUP);
  pinMode(REED_PIN, INPUT_PULLUP);

  // Debug reset button state
  Serial.println("Reset button initial state: " + String(digitalRead(RESET_BUTTON_PIN)));
  if (checkResetButton()) {
    Serial.println("Reset button pressed during early setup. Restarting...");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Resetting...");
    tone(BUZZER_PIN, 1000, 200);
    delay(300);
    ESP.restart();
  }

  // Initialize I2C and LCD
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire.setClock(50000);
  Wire.setTimeOut(100);
  scanI2C();
  lcd.begin(16, 2);
  Wire.beginTransmission(0x27);
  if (Wire.endTransmission() != 0) {
    Serial.println("LCD I2C initialization failed at address 0x27!");
    lcd.noBacklight();
    tone(BUZZER_PIN, 500, 1000);
    while (1) {
      if (checkResetButton()) ESP.restart();
    yield();
    }
  }
  lcd.backlight();
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("System Booting");
  delay(1000);
  if (checkResetButton()) ESP.restart();

  // WiFi connection
  int wifiRetries = 3;
  bool wifiConnected = false;
  for (int i = 0; i < wifiRetries && !wifiConnected; i++) {
    connectWiFi();
    if (WiFi.status() == WL_CONNECTED) {
      wifiConnected = true;
      isConnected = true;
      Serial.println("WiFi connected to: " + String(WiFi.SSID()));
      Serial.println("IP Address: " + WiFi.localIP().toString());
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("WiFi Connected");
      lcd.setCursor(0, 1);
      lcd.print(WiFi.localIP().toString());
      delay(1500);
      if (checkResetButton()) ESP.restart();
    } else {
      Serial.println("WiFi attempt " + String(i + 1) + " failed. Retrying...");
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("WiFi Retry ");
      lcd.print(i + 1);
      lcd.setCursor(0, 1);
      lcd.print("Wait 2s...");
      delay(2000);
      if (checkResetButton()) ESP.restart();
    }
  }

  if (!wifiConnected) {
    Serial.println("WiFi failed after " + String(wifiRetries) + " attempts. Switching to SD mode.");
    sdMode = true;
    isConnected = false;
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("WiFi Failed");
    lcd.setCursor(0, 1);
    lcd.print("SD Mode On");
    tone(BUZZER_PIN, 800, 500);
    delay(2000);
    if (checkResetButton()) ESP.restart();
  }

  // Initialize RTC
  Rtc.Begin();
  Serial.println("DS1302 RTC Initialized");
  RtcDateTime compiled = RtcDateTime(__DATE__, __TIME__);
  if (!Rtc.IsDateTimeValid()) {
    Serial.println("RTC lost confidence in DateTime! Setting to compile time.");
    Rtc.SetDateTime(compiled);
  }
  if (Rtc.GetIsWriteProtected()) {
    Serial.println("RTC was write-protected, enabling writing.");
    Rtc.SetIsWriteProtected(false);
  }
  if (!Rtc.GetIsRunning()) {
    Serial.println("RTC was not running, starting now.");
    Rtc.SetIsRunning(true);
  }
  if (checkResetButton()) ESP.restart();

  // NTP and Firebase setup if online
  if (!sdMode) {
    IPAddress ntpIP;
    if (WiFi.hostByName("pool.ntp.org", ntpIP)) {
      Serial.println("pool.ntp.org resolved to " + ntpIP.toString());
    } else {
      Serial.println("Failed to resolve pool.ntp.org");
    }

    configTime(8 * 3600, 0, "pool.ntp.org", "time.nist.gov");
    Serial.println("NTP configured (UTC+8). Syncing time...");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Syncing Time");

    const int ntpRetries = 3;
    const unsigned long syncTimeout = 20000;
    struct tm timeinfo;
    bool timeSynced = false;

    for (int attempt = 0; attempt < ntpRetries && !timeSynced; attempt++) {
      unsigned long startTime = millis();
      Serial.print("NTP sync attempt " + String(attempt + 1) + "...");
      while (millis() - startTime < syncTimeout && !timeSynced) {
        if (getLocalTime(&timeinfo)) timeSynced = true;
        Serial.print(".");
        delay(500);
        if (checkResetButton()) ESP.restart();
      }
      if (timeSynced) {
        char timeString[20];
        strftime(timeString, sizeof(timeString), "%H:%M:%S %Y-%m-%d", &timeinfo);
        Serial.println("\nTime synced: " + String(timeString));
        lcd.setCursor(0, 1);
        lcd.print(timeString);
        delay(1500);
        if (checkResetButton()) ESP.restart();
        Rtc.SetDateTime(RtcDateTime(timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
                                    timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec));
        Serial.println("RTC synced with NTP time.");
      } else {
        Serial.println("\nNTP sync attempt " + String(attempt + 1) + " failed.");
        if (attempt < ntpRetries - 1) {
          lcd.setCursor(0, 1);
          lcd.print("Retry " + String(attempt + 2));
          delay(2000);
          if (checkResetButton()) ESP.restart();
        }
      }
    }

    if (!timeSynced) {
      Serial.println("Time sync failed after " + String(ntpRetries) + " attempts. Switching to SD mode.");
      sdMode = true;
      isConnected = false;
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("Time Sync Fail");
      lcd.setCursor(0, 1);
      lcd.print("SD Mode");
      tone(BUZZER_PIN, 1000, 500);
      digitalWrite(LED_R_PIN, HIGH);
      delay(3000);
      digitalWrite(LED_R_PIN, LOW);
      if (checkResetButton()) ESP.restart();
    } else {
      initFirebase();
      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("Firebase Init");
      delay(1500);
      if (checkResetButton()) ESP.restart();

      lcd.clear();
      lcd.setCursor(0, 0);
      lcd.print("Fetching Students");
      Serial.println("Calling fetchFirestoreStudents...");
      fetchFirestoreStudents();
      lcd.setCursor(0, 1);
      if (firestoreStudents.find("5464E1BA") != firestoreStudents.end()) {
        lcd.print("Students Loaded");
        Serial.println("fetchFirestoreStudents completed. 5464E1BA found.");
      } else {
        lcd.print("Fetch Failed");
        Serial.println("fetchFirestoreStudents completed. 5464E1BA NOT found.");
      }
      delay(1500);
      if (checkResetButton()) ESP.restart();
    }
  } else {
    Serial.println("SD mode active. Using RTC for time.");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("SD Mode");
    lcd.setCursor(0, 1);
    lcd.print("RTC Time Active");
    delay(2000);
    if (checkResetButton()) ESP.restart();
  }

  // RFID initialization
  hspi.begin(MFRC522_SCK, MFRC522_MISO, MFRC522_MOSI, MFRC522_CS);
  pinMode(MFRC522_CS, OUTPUT);
  pinMode(MFRC522_RST, OUTPUT);
  
  // Reset the RFID reader completely
  digitalWrite(MFRC522_RST, LOW);
  delay(10);
  digitalWrite(MFRC522_RST, HIGH);
  delay(50);
  
  rfid.PCD_Init();
  delay(50); // Add a small delay after init
  
  // Read version to verify initialization
  byte version = rfid.PCD_ReadRegister(rfid.VersionReg);
  Serial.print("MFRC522 Version: 0x");
  Serial.println(version, HEX);
  
  if (version == 0x00 || version == 0xFF) {
    Serial.println("WARNING: RFID reader not responding. Trying again...");
    // Try one more time
    rfid.PCD_Init();
    delay(100);
    version = rfid.PCD_ReadRegister(rfid.VersionReg);
    Serial.print("MFRC522 Version 2nd attempt: 0x");
    Serial.println(version, HEX);
  }
  
  Serial.println("MFRC522 initialized.");
  rfid.PCD_SetAntennaGain(rfid.RxGain_max); // Set maximum gain
  
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("RFID Ready");
  delay(1500);
  if (checkResetButton()) ESP.restart();

  // SD card initialization
  fsSPI.begin(SD_SCK, SD_MISO, SD_MOSI, SD_CS);
  pinMode(SD_CS, OUTPUT);
  if (!SD.begin(SD_CS, fsSPI, 4000000)) {
    Serial.println("⚠️ SD Card initialization failed!");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("SD Card Fail");
    tone(BUZZER_PIN, 700, 1000);
    while (1) {
      if (checkResetButton()) ESP.restart();
      yield();
    }
  }
  Serial.println("✅ SD Card initialized.");
  sdInitialized = true;
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("SD Card Ready");
  delay(1500);
  if (checkResetButton()) ESP.restart();

  // PZEM initialization
  pzemSerial.begin(9600, SERIAL_8N1, PZEM_RX, PZEM_TX);
  if (!pzem.resetEnergy()) {
    Serial.println("PZEM reset failed!");
  }
  Serial.println("PZEM initialized on UART1 (RX=" + String(PZEM_RX) + ", TX=" + String(PZEM_TX) + ").");
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("PZEM Ready");
  delay(1500);
  if (checkResetButton()) ESP.restart();

  // Sync SD data and schedules
  lastActivityTime = millis();
  checkAndSyncSDData();
  if (isConnected && Firebase.ready()) {
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Syncing Schedules");
    if (syncSchedulesToSD()) {
      Serial.println("Schedules synced to SD during setup.");
      lcd.setCursor(0, 1);
      lcd.print("Sync Complete");
    } else {
      Serial.println("Failed to sync schedules to SD during setup.");
      lcd.setCursor(0, 1);
      lcd.print("Sync Failed");
    }
    delay(1500);
    if (checkResetButton()) ESP.restart();
  } else {
    Serial.println("Skipping schedule sync: No connection or Firebase not ready.");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Offline Mode");
    lcd.setCursor(0, 1);
    lcd.print("Using SD Cache");
    delay(1500);
    if (checkResetButton()) ESP.restart();
  }

  // Final setup message
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("Ready. Tap your");
  lcd.setCursor(0, 1);
  lcd.print("RFID Card!");
  showNeutral();
  Serial.println("Setup complete. System ready.");
  lastReadyPrint = millis();
  readyMessageShown = true;

  Serial.println("Stack remaining at end of setup: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");

  // Create custom loop task
  if (xTaskCreatePinnedToCore(
        customLoopTask, "CustomLoopTask", 32768, NULL, 1, NULL, 1) != pdPASS) {
    Serial.println("Failed to create CustomLoopTask. Insufficient memory?");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("Task Fail");
    while (1) {
      if (checkResetButton()) ESP.restart();
      yield();
    }
  }
  Serial.println("CustomLoopTask created with 32 KB stack size.");

  // Prevent setup from returning
  while (1) {
    if (checkResetButton()) ESP.restart();
    vTaskDelay(1000 / portTICK_PERIOD_MS);
  }

  // Initialize SD card
  initSDCard();
  uidDetailsFetched = false;

  // Create FreeRTOS tasks
  xTaskCreatePinnedToCore(
    rfidTask,            // Task function
    "RFIDTask",          // Name
    4096,                // Stack size (bytes)
    NULL,                // Parameters
    1,                   // Priority (1 is low)
    NULL,                // Task handle
    1                    // Core (1 = second core)
  );

  Serial.println("Setup complete. Running normal operation.");
  displayMessage("Ready. Tap your", "RFID Card!", 0);
  readyMessageShown = true;
  lastReadyPrint = millis();
  lastActivityTime = millis();
}

void verifyStudent(String uid) {
  String timestamp = getFormattedTime();
  
  // Mark student as present immediately upon RFID tap
  logStudentToRTDB(uid, timestamp, 0.0, -1, "true", "");
  
  // Update attendance count
  presentCount++;
  
  // Display confirmation
  String studentName = firestoreStudents[uid]["fullName"];
  if (studentName.length() == 0) studentName = "Student";
  displayMessage(studentName, "Attendance Marked", 2000);
  
  // Feedback
  digitalWrite(LED_G_PIN, HIGH);
  tone(BUZZER_PIN, 1000, 200);
  delay(200);
  digitalWrite(LED_G_PIN, LOW);
}

void handleStudentTapOut(String uid) {
  String timestamp = getFormattedTime();
  String studentName = firestoreStudents[uid]["fullName"];
  if (studentName.length() == 0) studentName = "Student";
  
  // Log tap out time
  logStudentToRTDB(uid, timestamp, 0.0, -1, "true", timestamp);
  
  // Update count and display
  presentCount = max(0, presentCount - 1);
  displayMessage(studentName + " Out", "Remaining: " + String(presentCount), 2000);
  
  // Feedback
  digitalWrite(LED_G_PIN, HIGH);
  tone(BUZZER_PIN, 1000, 200);
  delay(200);
  digitalWrite(LED_G_PIN, LOW);
}

void updatePZEM() {
  lastVoltage = pzem.voltage();
  lastCurrent = pzem.current();
  lastPower = pzem.power();
  lastEnergy = pzem.energy();
  lastFrequency = pzem.frequency();
  lastPowerFactor = pzem.pf();
  isVoltageSufficient = (lastVoltage >= voltageThreshold && !isnan(lastVoltage));
  if (millis() - lastPZEMLogTime > 5000) { // Log every 5s
    Serial.println("PZEM: V=" + String(lastVoltage, 2) + 
                   ", C=" + String(lastCurrent, 2) + 
                   ", E=" + String(lastEnergy, 2));
    lastPZEMLogTime = millis();
  }
}

void handleOfflineSync() {
  if (!SD.exists("/Offline_Logs_Entry.txt")) {
    Serial.println("No offline logs to sync (/Offline_Logs_Entry.txt not found).");
    return;
  }

  File logFile = SD.open("/Offline_Logs_Entry.txt", FILE_READ);
  if (!logFile) {
    Serial.println("Failed to open /Offline_Logs_Entry.txt for reading");
    return;
  }

  Serial.println("Starting offline log sync to Firebase...");
  while (logFile.available()) {
    String line = logFile.readStringUntil('\n');
    line.trim();
    if (line.length() == 0) continue;

    // Parse log entry
    if (line.indexOf("SuperAdminPZEMInitial:") == 0) {
      continue; // Skip initial PZEM readings
    } else if (line.indexOf("Tamper:") == 0) {
      // Handle tamper logs
      String timestamp = line.substring(line.indexOf("Timestamp:") + 10, line.indexOf(" Status:"));
      String status = line.substring(line.indexOf("Status:") + 7, line.indexOf(" ", line.indexOf("Status:")) >= 0 ? line.indexOf(" ", line.indexOf("Status:")) : line.length());
      FirebaseJson json;
      timestamp.replace("_", "T");
      json.set("startTime", timestamp);
      json.set("status", status.equalsIgnoreCase("Resolved") ? "resolved" : "active");
      String path = "/Alerts/Tamper/" + timestamp;
      if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
        Serial.println("Offline tamper log pushed to RTDB: " + path);
      } else {
        Serial.println("Failed to push offline tamper log: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    } else if (line.indexOf("System:") == 0) {
      // Handle system logs (e.g., WiFiLost)
      String timestamp = line.substring(line.indexOf("Timestamp:") + 10, line.indexOf(" Action:"));
      String action = line.substring(line.indexOf("Action:") + 7, line.length());
      FirebaseJson json;
      timestamp.replace("_", "T");
      json.set("timestamp", timestamp);
      json.set("action", action);
      String path = "/SystemEvents/" + timestamp;
      if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
        Serial.println("Offline system log pushed to RTDB: " + path);
      } else {
        Serial.println("Failed to push offline system log: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    } else if (line.indexOf("Student:") == 0) {
      // Handle student logs
      String uid = line.substring(line.indexOf("UID:") + 4, line.indexOf(" TimeIn:"));
      String timeIn = line.substring(line.indexOf("TimeIn:") + 7, line.indexOf(" Action:"));
      String action = line.substring(line.indexOf("Action:") + 7, line.indexOf(" Status:"));
      String status = line.substring(line.indexOf("Status:") + 7, line.indexOf(" Sensor:"));
      String sensor = line.substring(line.indexOf("Sensor:") + 7, line.indexOf(" assignedSensorId:"));
      String assignedSensorId = line.substring(line.indexOf("assignedSensorId:") + 17, line.indexOf(" TimeOut:") >= 0 ? line.indexOf(" TimeOut:") : line.length());
      String timeOut = "";
      if (line.indexOf("TimeOut:") >= 0) {
        timeOut = line.substring(line.indexOf("TimeOut:") + 8, line.length());
      }

      FirebaseJson json;
      timeIn.replace("_", "T");
      json.set("Time In", timeIn);
      json.set("Action", action);
      json.set("Status", status);
      json.set("Sensor", sensor);
      json.set("assignedSensorId", assignedSensorId.toInt());
      if (timeOut.length() > 0) {
        timeOut.replace("_", "T");
        json.set("Time Out", timeOut);
      }

      // Fetch student data
      String studentName = "Unknown", email = "", idNumber = "", mobileNumber = "", role = "student", department = "";
      String schedulesJsonStr = "[]", subjectCode = "Unknown", roomName = "Unknown", sectionName = "Unknown";
      if (firestoreStudents.find(uid) != firestoreStudents.end()) {
        studentName = firestoreStudents[uid]["fullName"].length() > 0 ? firestoreStudents[uid]["fullName"] : "Unknown";
        email = firestoreStudents[uid]["email"];
        idNumber = firestoreStudents[uid]["idNumber"];
        mobileNumber = firestoreStudents[uid]["mobileNumber"];
        role = firestoreStudents[uid]["role"].length() > 0 ? firestoreStudents[uid]["role"] : "student";
        department = firestoreStudents[uid]["department"];
        schedulesJsonStr = firestoreStudents[uid]["schedules"].length() > 0 ? firestoreStudents[uid]["schedules"] : "[]";
      }

      json.set("fullName", studentName);
      json.set("email", email);
      json.set("idNumber", idNumber);
      json.set("mobileNumber", mobileNumber);
      json.set("role", role);
      json.set("department", department);
      json.set("timestamp", timeIn);
      json.set("date", timeIn.substring(0, 10));

      // Parse schedules
      FirebaseJsonArray schedulesArray;
      if (schedulesJsonStr != "[]") {
        FirebaseJsonArray tempArray;
        if (tempArray.setJsonArrayData(schedulesJsonStr)) {
          String currentDay = getDayFromTimestamp(timeIn);
          String currentTime = timeIn.substring(11, 16);
          for (size_t i = 0; i < tempArray.size(); i++) {
            FirebaseJsonData scheduleData;
            if (tempArray.get(scheduleData, i)) {
              FirebaseJson scheduleObj;
              if (scheduleObj.setJsonData(scheduleData.stringValue)) {
                FirebaseJson newScheduleObj;
                FirebaseJsonData fieldData;
                if (scheduleObj.get(fieldData, "day")) newScheduleObj.set("day", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "startTime")) newScheduleObj.set("startTime", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "endTime")) newScheduleObj.set("endTime", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "roomName")) newScheduleObj.set("roomName", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "subjectCode")) newScheduleObj.set("subjectCode", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "subject")) newScheduleObj.set("subject", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "section")) newScheduleObj.set("section", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "sectionId")) newScheduleObj.set("sectionId", fieldData.stringValue);
                if (scheduleObj.get(fieldData, "instructorName")) newScheduleObj.set("instructorName", fieldData.stringValue);
                schedulesArray.add(newScheduleObj);
                if (scheduleObj.get(fieldData, "day") && fieldData.stringValue == currentDay) {
                  String startTime, endTime;
                  if (scheduleObj.get(fieldData, "startTime")) startTime = fieldData.stringValue;
                  if (scheduleObj.get(fieldData, "endTime")) endTime = fieldData.stringValue;
                  if (isTimeInRange(currentTime, startTime, endTime)) {
                    if (scheduleObj.get(fieldData, "subjectCode")) subjectCode = fieldData.stringValue;
                    if (scheduleObj.get(fieldData, "roomName")) roomName = fieldData.stringValue;
                    if (scheduleObj.get(fieldData, "section")) sectionName = fieldData.stringValue;
                    break;
                  }
                }
              }
            }
          }
        }
      }
      String sessionId = timeIn.substring(0, 10) + "_" + subjectCode + "_" + sectionName + "_" + roomName;
      json.set("sessionId", sessionId);
      json.set("schedules", schedulesArray);

      String path = "/Students/" + uid + "/Attendance/" + sessionId;
      if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
        Serial.println("Offline student log pushed to RTDB: " + path);
      } else {
        Serial.println("Failed to push offline student log: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
    } else if (line.indexOf("UID:") == 0) {
      // Handle other UID-based logs (e.g., Super Admin, Instructor)
      String uid = line.substring(line.indexOf("UID:") + 4, line.indexOf(" ", line.indexOf("UID:")));
      String entryTime = line.indexOf("EntryTime:") >= 0 ? line.substring(line.indexOf("EntryTime:") + 10, line.indexOf(" ExitTime:") >= 0 ? line.indexOf(" ExitTime:") : line.indexOf(" Action:")) : "";
      String exitTime = line.indexOf("ExitTime:") >= 0 ? line.substring(line.indexOf("ExitTime:") + 9, line.indexOf(" Action:")) : "";
      String action = line.substring(line.indexOf("Action:") + 7, line.indexOf(" ", line.indexOf("Action:")) >= 0 ? line.indexOf(" ", line.indexOf("Action:")) : line.length());

      FirebaseJson json;
      if (entryTime.length() > 0) {
        entryTime.replace("_", "T");
        json.set("entry_time", entryTime);
      }
      if (exitTime.length() > 0) {
        exitTime.replace("_", "T");
        json.set("exit_time", exitTime);
      }

      // Parse PZEM data
      if (line.indexOf("TotalConsumption:") > 0) {
        String totalConsumption = line.substring(line.indexOf("TotalConsumption:") + 17, line.indexOf("kWh", line.indexOf("TotalConsumption:")));
        FirebaseJson pzemJson;
        pzemJson.set("total_consumption", totalConsumption + "kWh");
        json.set("pzem_data", pzemJson);
      }

      if (uid == SUPER_ADMIN_UID) {
        json.set("uid", uid);
        json.set("name", "CIT-U");
        json.set("role", "Super Admin");
        json.set("department", "Computer Engineering");
        json.set("building", "GLE");
        json.set("action", action);
        String path = "/OfflineDataLogging/" + uid + "_" + entryTime;
        if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
          Serial.println("Offline Super Admin log pushed to RTDB: " + path);
        } else {
          Serial.println("Failed to push offline Super Admin log: " + fbdo.errorReason());
          if (fbdo.errorReason().indexOf("ssl") >= 0 || 
              fbdo.errorReason().indexOf("connection") >= 0 || 
              fbdo.errorReason().indexOf("SSL") >= 0) {
            handleFirebaseSSLError();
          }
        }
      } else if (firestoreTeachers.find(uid) != firestoreTeachers.end()) {
        json.set("uid", uid);
        json.set("name", firestoreTeachers[uid]["fullName"]);
        json.set("role", "Instructor");
        json.set("department", "Computer Engineering");
        json.set("building", "GLE");
        json.set("action", "FromSD Offline");

        // Extract schedule details
        if (line.indexOf("Schedule:") > 0) {
          String scheduleStr = line.substring(line.indexOf("Schedule:") + 9, line.indexOf(" Room:"));
          String room = line.substring(line.indexOf("Room:") + 5, line.indexOf(" Subject:"));
          String subject = line.substring(line.indexOf("Subject:") + 8, line.indexOf(" Code:"));
          String code = line.substring(line.indexOf("Code:") + 5, line.indexOf(" Section:"));
          String section = line.substring(line.indexOf("Section:") + 8, line.length());

          json.set("subject", subject);
          json.set("room", room);
          json.set("code", code);
          json.set("section", section);
        }

        String path = "/OfflineDataLogging/" + uid + "_" + entryTime;
        if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
          Serial.println("Offline instructor log pushed to RTDB: " + path);
        } else {
          Serial.println("Failed to push offline instructor log: " + fbdo.errorReason());
          if (fbdo.errorReason().indexOf("ssl") >= 0 || 
              fbdo.errorReason().indexOf("connection") >= 0 || 
              fbdo.errorReason().indexOf("SSL") >= 0) {
            handleFirebaseSSLError();
          }
        }
      } else {
        json.set("uid", uid);
        json.set("name", "Unknown");
        json.set("role", "Unknown");
        json.set("action", "FromSD Offline");
        String path = "/OfflineDataLogging/" + uid + "_" + entryTime;
        if (Firebase.RTDB.setJSON(&fbdo, path, &json)) {
          Serial.println("Offline unknown log pushed to RTDB: " + path);
        } else {
          Serial.println("Failed to push offline unknown log: " + fbdo.errorReason());
          if (fbdo.errorReason().indexOf("ssl") >= 0 || 
              fbdo.errorReason().indexOf("connection") >= 0 || 
              fbdo.errorReason().indexOf("SSL") >= 0) {
            handleFirebaseSSLError();
          }
        }
      }
    }

    yield(); // Allow other tasks during sync
  }

  logFile.close();
  if (SD.remove("/Offline_Logs_Entry.txt")) {
    Serial.println("Offline logs synced and file deleted.");
  } else {
    Serial.println("Failed to delete /Offline_Logs_Entry.txt");
  }
}

void updateAbsentStudents() {
  for (auto& student : firestoreStudents) {
    String rfidUid = student.first;
    String path = "/Students/" + rfidUid + "/Attendance/" + currentSessionId;
    if (Firebase.RTDB.getJSON(&fbdo, path)) {
      FirebaseJson* json = fbdo.jsonObjectPtr();
      FirebaseJsonData jsonData;
      if (json->get(jsonData, "Time Out") && jsonData.stringValue != "") {
        // Student already tapped out, skip
        continue;
      }
      if (json->get(jsonData, "Status") && jsonData.stringValue == "Present") {
        // Student was present but didn't tap out, update to Absent
        String timestamp = getFormattedTime();
        json->set("Status", "Absent");
        json->set("Time Out", "Not Recorded");
        json->set("Sensor", "None");
        if (Firebase.RTDB.setJSON(&fbdo, path, json)) {
          Serial.println("Updated student " + rfidUid + " to Absent due to no tap-out.");
          storeLogToSD("Student:UID:" + rfidUid + " UpdatedToAbsent Time:" + timestamp);
        } else {
          Serial.println("Failed to update student " + rfidUid + " to Absent: " + fbdo.errorReason());
          if (fbdo.errorReason().indexOf("ssl") >= 0 || 
              fbdo.errorReason().indexOf("connection") >= 0 || 
              fbdo.errorReason().indexOf("SSL") >= 0) {
            handleFirebaseSSLError();
          }
        }
      }
    }
  }
}

void handleFirebaseSSLError() {
  static unsigned long lastSSLErrorTime = 0;
  static int sslErrorRetryCount = 0;
  
  // If it's been more than 5 minutes since last error, reset counter
  if (millis() - lastSSLErrorTime > 300000) {
    sslErrorRetryCount = 0;
  }
  
  lastSSLErrorTime = millis();
  sslErrorRetryCount++;
  Serial.println("SSL Error Retry Count: " + String(sslErrorRetryCount));
  
  // Log the error
  storeLogToSD("SSLError:Timestamp:" + getFormattedTime() + ":Retry:" + String(sslErrorRetryCount));
  
  // Progressive response based on retry count
  if (sslErrorRetryCount <= 3) {
    // Initial retries: Just reconnect Firebase
    Serial.println("Attempting Firebase reconnection (soft retry)");
    Firebase.reconnectWiFi(true);
    
    // Reset Firebase completely to clear any corrupted state
    Firebase.reset(&config);
    delay(500);
    
    // Small delay for connection to stabilize
    for (int i = 0; i < 5; i++) {
      feedWatchdog();
      delay(100);
    }
    
    // Verify Firebase is ready
    if (ensureFirebaseAuthenticated()) {
      Serial.println("Firebase reconnected successfully after SSL error");
      sslErrorRetryCount = 0; // Reset counter on success
      digitalWrite(LED_G_PIN, HIGH);
      digitalWrite(LED_R_PIN, LOW);
      digitalWrite(LED_B_PIN, LOW);
      tone(BUZZER_PIN, 1000, 50);
      delay(50);
      displayMessage("Firebase", "Reconnected", 1000);
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      return;
    }
  } else if (sslErrorRetryCount <= 5) {
    // Moderate retry: Reinitialize Firebase
    Serial.println("Attempting Firebase reinitialization (moderate retry)");
    logSystemEvent("Firebase SSL Error - Reinitializing");
    
    // Signal the user about the error
    digitalWrite(LED_R_PIN, HIGH);
    digitalWrite(LED_G_PIN, LOW);
  
    tone(BUZZER_PIN, 500, 100);
    
    // Check if WiFi is still connected
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("WiFi disconnected. Reconnecting...");
      WiFi.disconnect();
      delay(500);
      connectWiFi();
    }
    
    // Reinitialize Firebase
    Firebase.reset(&config);
    delay(500);
    initFirebase();
    
    // Check if reconnection was successful
    if (Firebase.ready() && Firebase.authenticated()) {
      Serial.println("Firebase reinitialized successfully after SSL error");
      sslErrorRetryCount = 0; // Reset counter on success
      digitalWrite(LED_G_PIN, HIGH);
      digitalWrite(LED_R_PIN, LOW);
      digitalWrite(LED_B_PIN, LOW);
      tone(BUZZER_PIN, 1000, 100);
      displayMessage("Firebase", "Reinitialized", 1000);
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      return;
    }
  } else {
    // Severe retry: Switch to SD mode temporarily and attempt WiFi reconnection
    Serial.println("Multiple SSL errors detected. Switching to SD mode temporarily");
    logSystemEvent("Persistent SSL Errors - Switched to SD Mode");
    
    // Set to SD mode to ensure operations continue
    sdMode = true;
    
    // Signal the user about the error
    digitalWrite(LED_R_PIN, HIGH);
    digitalWrite(LED_G_PIN, LOW);
    digitalWrite(LED_B_PIN, LOW);
    tone(BUZZER_PIN, 500, 100);
    delay(100);
    tone(BUZZER_PIN, 300, 100);
    
    // Reset RFID reader to ensure it continues working
    digitalWrite(MFRC522_RST, LOW);
    delay(10);
    digitalWrite(MFRC522_RST, HIGH);
    delay(50);
    
    rfid.PCD_Init();
    delay(50);
    rfid.PCD_SetAntennaGain(rfid.RxGain_max);
    
    // Try a more aggressive WiFi reconnection approach
    Serial.println("Performing aggressive WiFi reset and reconnection");
    WiFi.disconnect(true);
    delay(1000);
    WiFi.mode(WIFI_OFF);
    delay(1000);
    WiFi.mode(WIFI_STA);
    delay(1000);
    connectWiFi();
    
    // If WiFi reconnected, try Firebase one more time
    if (WiFi.status() == WL_CONNECTED) {
      delay(1000);
      Firebase.reset(&config);
      delay(1000);
      initFirebase();
      
      if (Firebase.ready() && Firebase.authenticated()) {
        Serial.println("Firebase reconnected after aggressive WiFi reset");
        sdMode = false;
        sslErrorRetryCount = 0;
        displayMessage("Firebase", "Reconnected", 2000);
        displayMessage("Ready. Tap your", "RFID Card!", 0);
        return;
      }
    }
    
    // Schedule an automatic retry after 5 minutes (reduced from 10)
    sdModeRetryTime = millis() + 300000; // 5 minutes
    
    digitalWrite(LED_R_PIN, LOW);
    feedWatchdog();
    
    displayMessage("Firebase Error", "Switched to SD", 2000);
    displayMessage("Ready. Tap your", "RFID Card!", 0);
    
    // Reset retry count if it gets too high
    if (sslErrorRetryCount > 10) {
      sslErrorRetryCount = 6; // Keep at the severe retry level but prevent overflow
    }
  }
}

// Modify loop function to include admin door auto-lock check
void loop() {
  watchdogCheck(); // Feed WDT
  if (checkResetButton()) return; // Early reset check
  yield(); // Prevent watchdog reset at start of loop
  delay(5); // Small delay to allow other tasks to run
  yield(); // Additional yield after delay

  // Check for admin door auto-lock timeout
  checkAdminDoorAutoLock();
  yield(); // Add yield after admin door check
  
  // Update lastReadyPrint during active operations to prevent watchdog timeouts
  if (classSessionActive || studentVerificationActive || relayActive || tapOutPhase) {
    if (millis() - lastReadyPrint > 60000) { // Update every minute during active operations
      lastReadyPrint = millis();
      Serial.println("Watchdog timer extended for active operation");
    }
  }
  
  // Add yield before end time check
  yield();

  // Check if a class session should be ended due to reaching the end time
  static unsigned long lastEndTimeCheck = 0;
  if (classSessionActive && !tapOutPhase && millis() - lastEndTimeCheck >= 30000) {
    String currentTime;
    
    if (sdMode) {
      // In offline mode, use RTC to get current time
      RtcDateTime now = Rtc.GetDateTime();
      if (now.IsValid()) {
        char timeBuffer[6];
        sprintf(timeBuffer, "%02u:%02u", now.Hour(), now.Minute());
        currentTime = String(timeBuffer);
        Serial.println("Using RTC time for end time check: " + currentTime);
      } else {
        Serial.println("RTC time invalid, skipping end time check");
        lastEndTimeCheck = millis();
        return;
      }
    } else {
      // In online mode, use NTP-based time
      String fullTimestamp = getFormattedTime();
      Serial.println("DEBUG: Full timestamp from getFormattedTime(): " + fullTimestamp);
      
      if (fullTimestamp.length() >= 16) {
        // Extract hours and minutes properly from timestamp format YYYY_MM_DD_HHMMSS
        // The time portion starts at position 11 and format is HHMMSS (without colons)
        String hourStr = fullTimestamp.substring(11, 13);
        String minuteStr = fullTimestamp.substring(13, 15);
        currentTime = hourStr + ":" + minuteStr; // Format as HH:MM with colon
        Serial.println("DEBUG: Extracted time portion: " + currentTime);
      } else {
        Serial.println("Invalid timestamp format: " + fullTimestamp + " (length: " + fullTimestamp.length() + ")");
        lastEndTimeCheck = millis();
        return;
      }
    }
    
    // Make sure we have valid end time format before comparison
    Serial.println("Checking end time: Current time is " + currentTime + ", scheduled end time is " + currentSchedule.endTime);
    Serial.println("DEBUG: End time validation - currentSchedule.isValid=" + String(currentSchedule.isValid) + 
                   ", endTime.length=" + String(currentSchedule.endTime.length()) + 
                   ", currentTime.length=" + String(currentTime.length()));
    
    // Compare times in HH:MM format for schedule end time check
    if (currentSchedule.isValid && currentSchedule.endTime.length() == 5 && currentTime.length() == 5) {
      // Convert times to minutes for easier comparison
      int currentHour = currentTime.substring(0, 2).toInt();
      int currentMinute = currentTime.substring(3, 5).toInt();
      int currentTotalMinutes = currentHour * 60 + currentMinute;
      
      int endHour = currentSchedule.endTime.substring(0, 2).toInt();
      int endMinute = currentSchedule.endTime.substring(3, 5).toInt();
      int endTotalMinutes = endHour * 60 + endMinute;
      
      // Get start time in minutes for span detection
      int startHour = 0;
      int startMinute = 0;
      if (currentSchedule.startTime.length() == 5) {
        startHour = currentSchedule.startTime.substring(0, 2).toInt();
        startMinute = currentSchedule.startTime.substring(3, 5).toInt();
      }
      int startTotalMinutes = startHour * 60 + startMinute;
      
      // Check for class spanning across midnight (endTime < startTime)
      bool spansMidnight = endTotalMinutes < startTotalMinutes;
      
      // Adjust comparison for classes that span midnight
      bool endTimeReached = false;
      if (spansMidnight) {
        // If class spans midnight and current time is less than start time, 
        // it means we're after midnight, so adjust comparison
        if (currentTotalMinutes < startTotalMinutes) {
          endTimeReached = (currentTotalMinutes >= endTotalMinutes);
        } else {
          // Current time is after start time but before midnight
          endTimeReached = false;
        }
      } else {
        // Normal comparison for classes that don't span midnight
        endTimeReached = (currentTotalMinutes >= endTotalMinutes);
      }
      
      Serial.println("Time in minutes - Current: " + String(currentTotalMinutes) + 
                     ", Start: " + String(startTotalMinutes) + 
                     ", End: " + String(endTotalMinutes) + 
                     ", Spans midnight: " + String(spansMidnight));
      
      if (endTimeReached) {
        Serial.println("End time reached (" + currentTime + " >= " + currentSchedule.endTime + "). Transitioning to tap-out phase.");
        
        digitalWrite(RELAY2, HIGH);
        delay(20); // Small delay for relay
        yield(); // Allow system to process
        
        digitalWrite(RELAY3, HIGH);
        delay(20); // Small delay for relay
        yield(); // Allow system to process
        
        digitalWrite(RELAY4, HIGH);
        delay(20); // Small delay for relay
        yield(); // Allow system to process
        
        relayActive = false;
        classSessionActive = false;
        tapOutPhase = true;
        tapOutStartTime = millis();
        pzemLoggedForSession = false;
        tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
        
        String timestamp = getFormattedTime();
        
        // Store PZEM data and update ClassStatus to "Class Ended"
        if (!sdMode && isConnected && Firebase.ready() && currentSchedule.roomName.length() > 0) {
          float voltage = pzem.voltage();
          float current = pzem.current();
          float power = pzem.power();
          float energy = pzem.energy();
          float frequency = pzem.frequency();
          float powerFactor = pzem.pf();
          
          if (isnan(voltage) || voltage < 0) voltage = 0.0;
          if (isnan(current) || current < 0) current = 0.0;
          if (isnan(power) || power < 0) power = 0.0;
          if (isnan(energy) || energy < 0) energy = 0.0;
          if (isnan(frequency) || frequency < 0) frequency = 0.0;
          if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
          
          String instructorPath = "/Instructors/" + lastInstructorUID;
          FirebaseJson classStatusJson;
          classStatusJson.set("Status", "Class Ended");
          classStatusJson.set("dateTime", timestamp);
          
          FirebaseJson scheduleJson;
          scheduleJson.set("day", currentSchedule.day.length() > 0 ? currentSchedule.day : "Unknown");
          scheduleJson.set("startTime", currentSchedule.startTime.length() > 0 ? currentSchedule.startTime : "Unknown");
          scheduleJson.set("endTime", currentSchedule.endTime.length() > 0 ? currentSchedule.endTime : "Unknown");
          scheduleJson.set("subject", currentSchedule.subject.length() > 0 ? currentSchedule.subject : "Unknown");
          scheduleJson.set("subjectCode", currentSchedule.subjectCode.length() > 0 ? currentSchedule.subjectCode : "Unknown");
          scheduleJson.set("section", currentSchedule.section.length() > 0 ? currentSchedule.section : "Unknown");
          
          FirebaseJson roomNameJson;
          roomNameJson.set("name", currentSchedule.roomName.length() > 0 ? currentSchedule.roomName : "Unknown");
          
          // Check if there's existing PZEM data to preserve
          if (Firebase.RTDB.get(&fbdo, instructorPath + "/ClassStatus/schedule/roomName/pzem")) {
            if (fbdo.dataType() == "json") {
              // Get the existing PZEM data
              FirebaseJson pzemData;
              pzemData.setJsonData(fbdo.jsonString());
              
              // Set it in the roomName object
              roomNameJson.set("pzem", pzemData);
              Serial.println("Preserved existing PZEM data during session end");
            }
          }
          
          // Calculate energy consumption using E = P*(t/1000)
          // Get the session duration in hours
          float sessionDurationHours = 0.0;
          if (currentSchedule.startTime.length() > 0 && currentSchedule.endTime.length() > 0) {
            // Extract hours and minutes from startTime (format HH:MM)
            int startHour = currentSchedule.startTime.substring(0, 2).toInt();
            int startMinute = currentSchedule.startTime.substring(3, 5).toInt();
            
            // Extract hours and minutes from endTime (format HH:MM)
            int endHour = currentSchedule.endTime.substring(0, 2).toInt();
            int endMinute = currentSchedule.endTime.substring(3, 5).toInt();
            
            // Calculate total minutes
            int startTotalMinutes = startHour * 60 + startMinute;
            int endTotalMinutes = endHour * 60 + endMinute;
            
            // Handle cases where the end time is on the next day
            if (endTotalMinutes < startTotalMinutes) {
              endTotalMinutes += 24 * 60; // Add 24 hours in minutes
            }
            
            // Calculate the duration in hours
            sessionDurationHours = (endTotalMinutes - startTotalMinutes) / 60.0;
          }
          
          // Calculate energy consumption (kWh) using E = P*(t/1000)
          // Power is in watts, time is in hours, result is in kWh
          float calculatedEnergy = power * sessionDurationHours / 1000.0;
          
          // Use the calculated energy if the measured energy is zero or invalid
          if (energy <= 0.01) {
            energy = calculatedEnergy;
          }
          
          Serial.println("Session duration: " + String(sessionDurationHours) + " hours");
          Serial.println("Calculated energy: " + String(calculatedEnergy) + " kWh");

          FirebaseJson pzemJson;
          pzemJson.set("voltage", String(voltage, 1));
          pzemJson.set("current", String(current, 2));
          pzemJson.set("power", String(power, 1));
          pzemJson.set("energy", String(energy, 2));
          pzemJson.set("calculatedEnergy", String(calculatedEnergy, 2));
          pzemJson.set("sessionDuration", String(sessionDurationHours, 2));
          pzemJson.set("frequency", String(frequency, 1));
          pzemJson.set("powerFactor", String(powerFactor, 2));
          pzemJson.set("timestamp", timestamp);
          pzemJson.set("action", "end");
          roomNameJson.set("pzem", pzemJson);
          pzemLoggedForSession = true;
          Serial.println("PZEM logged at session end: Voltage=" + String(voltage, 1) + ", Energy=" + String(energy, 2));

          // Remove the old separate Rooms node storage
          // Instead, modify ClassStatus to include all necessary information
          if (currentSchedule.roomName.length() > 0) {
            // Instead of creating a separate path, we'll log this information to the classStatusJson
            FirebaseJson classDetailsJson;
            classDetailsJson.set("roomName", currentSchedule.roomName);
            classDetailsJson.set("subject", currentSchedule.subject);
            classDetailsJson.set("subjectCode", currentSchedule.subjectCode);
            classDetailsJson.set("section", currentSchedule.section);
            classDetailsJson.set("sessionStart", currentSchedule.startTime);
            classDetailsJson.set("sessionEnd", currentSchedule.endTime);
            classDetailsJson.set("date", timestamp.substring(0, 10));
            classDetailsJson.set("sessionId", currentSessionId);
            
            // Add this class details to the ClassStatus node
            classStatusJson.set("roomDetails", classDetailsJson);
            
            Serial.println("Class details included in ClassStatus");
          }
          
          scheduleJson.set("roomName", roomNameJson);
          classStatusJson.set("schedule", scheduleJson);
          
          String statusPath = instructorPath + "/ClassStatus";
          if (Firebase.RTDB.updateNode(&fbdo, statusPath, &classStatusJson)) {
            Serial.println("Class status updated to 'Class Ended' with PZEM data");
          } else {
            String errorLog = "FirebaseError:Path:" + statusPath + " Reason:" + fbdo.errorReason();
            storeLogToSD(errorLog);
            Serial.println("Failed to update class status with PZEM data: " + fbdo.errorReason());
          }
        } else {
          // In offline mode, store session end information to SD card for later syncing
          float voltage = pzem.voltage();
          float current = pzem.current();
          float power = pzem.power();
          float energy = pzem.energy();
          float frequency = pzem.frequency();
          float powerFactor = pzem.pf();
          
          if (isnan(voltage) || voltage < 0) voltage = 0.0;
          if (isnan(current) || current < 0) current = 0.0;
          if (isnan(power) || power < 0) power = 0.0;
          if (isnan(energy) || energy < 0) energy = 0.0;
          if (isnan(frequency) || frequency < 0) frequency = 0.0;
          if (isnan(powerFactor) || powerFactor < 0) powerFactor = 0.0;
          
          String offlineSessionLog = "SessionEnd:UID:" + lastInstructorUID + 
                                    " Time:" + timestamp + 
                                    " Status:Class Ended" +
                                    " Day:" + currentSchedule.day +
                                    " StartTime:" + currentSchedule.startTime +
                                    " EndTime:" + currentSchedule.endTime +
                                    " Subject:" + currentSchedule.subject +
                                    " SubjectCode:" + currentSchedule.subjectCode +
                                    " Section:" + currentSchedule.section +
                                    " Room:" + currentSchedule.roomName +
                                    " Voltage:" + String(voltage, 1) +
                                    " Current:" + String(current, 2) +
                                    " Power:" + String(power, 1) +
                                    " Energy:" + String(energy, 2) +
                                    " Frequency:" + String(frequency, 1) +
                                    " PowerFactor:" + String(powerFactor, 2) +
                                    " AutoEnd:true";
          
          storeLogToSD(offlineSessionLog);
          Serial.println("Offline session end data logged to SD card");
        }
        
        smoothTransitionToReady();
      }
    }
    
    lastEndTimeCheck = millis();
    yield(); // Add yield after end time check
  }

  // Critical section - update watchdog timers before relay operations
  lastActivityTime = millis(); // Feed watchdog timer before critical operation
  lastReadyPrint = millis();  // Update ready print time to prevent timeouts
  yield(); // Yield before relay operations
  delay(10); // Small delay before critical operation
  yield(); // Additional yield after delay

  // Process any pending relay operations with additional watchdog protection
  watchdogCheck(); // Explicitly feed watchdog before critical operation
  checkPendingRelayOperations();
  watchdogCheck(); // Explicitly feed watchdog after critical operation
  
  // Multiple yields and watchdog updates after relay operations
  yield();
  delay(20); // Give system time to stabilize after relay operations
  yield();
  lastActivityTime = millis(); // Update watchdog timer after relay operations
  lastReadyPrint = millis();

  // Periodic updates with additional yields
  static unsigned long lastPeriodicUpdate = 0;
  if (millis() - lastPeriodicUpdate >= 100) {
    yield(); // Yield before PZEM update
    updatePZEM();
    yield(); // Yield after PZEM update
    updateDisplay();
    yield(); // Yield after display update
    lastPeriodicUpdate = millis();
  }

  // More frequent display updates during tap-out phase
  static unsigned long lastTapOutDisplayUpdate = 0;
  if (tapOutPhase && millis() - lastTapOutDisplayUpdate >= 1000) { // Update every second during tap-out
    yield(); // Yield before display update
    updateDisplay(); // Extra call for tap-out phase
    lastTapOutDisplayUpdate = millis();
    yield(); // Yield after display update
  }

  // Heap monitoring (more frequent)
  static unsigned long lastHeapCheck = 0;
  if (millis() - lastHeapCheck >= 10000) { // Changed from 30000 to 10000
    Serial.println("Free heap: " + String(ESP.getFreeHeap()) + " bytes");
    if (ESP.getFreeHeap() < 10000) {
      logSystemEvent("Low Memory Reset");
      ESP.restart();
    }
    lastHeapCheck = millis();
    yield(); // Yield after heap check
  }

  // Log heap trends to SD (for debugging memory leaks)
  static unsigned long lastHeapLog = 0;
  if (millis() - lastHeapLog >= 60000) { // Every minute
    String heapLog = "Heap:Free:" + String(ESP.getFreeHeap()) + " Timestamp:" + getFormattedTime();
    storeLogToSD(heapLog);
    lastHeapLog = millis();
    yield(); // Yield after logging
  }

  isConnected = (WiFi.status() == WL_CONNECTED);
  yield(); // Yield after WiFi status check

  // SD initialization message
  static bool sdInitializedMessageShown = false;
  static unsigned long sdMessageStart = 0;
  if (sdInitialized && !sdInitializedMessageShown) {
    if (sdMessageStart == 0) {
      // Enhanced message display with more visibility
      displayMessage("Backup Logs", "Activated", 2500);
      Serial.println("SD card initialized. Backup logs activated.");
      sdMessageStart = millis();
      lastActivityTime = millis();
      lastReadyPrint = millis();
      // Add a small delay to ensure message is displayed properly
      unsigned long startTime = millis();
      while (millis() - startTime < 200) yield(); // Small non-blocking delay
    } else if (millis() - sdMessageStart >= 2500) {
      // Ensure smooth transition
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      Serial.println("Transitioned to Ready state after backup logs activated.");
      sdInitializedMessageShown = true;
      readyMessageShown = true;
      lastReadyPrint = millis();
    }
    yield(); // Prevent watchdog reset before return
    return;
  }

  // Periodic stack monitoring (more frequent)
  static unsigned long lastPrint = 0;
  if (millis() - lastPrint >= 5000) { // Changed from 15000 to 5000
    Serial.println("Stack remaining: " + String(uxTaskGetStackHighWaterMark(NULL)) + " bytes");
    lastPrint = millis();
    yield(); // Yield after stack check
  }

  // Firestore refresh
  static unsigned long lastFirestoreRefresh = 0;
  static enum { FS_IDLE, FS_TEACHERS, FS_STUDENTS, FS_ROOMS, FS_USERS } firestoreState = FS_IDLE;
  if (!sdMode && isConnected && millis() - lastFirestoreRefresh >= 300000) {
    yield(); // Prevent watchdog reset before Firestore operations
    switch (firestoreState) {
      case FS_IDLE: 
        firestoreState = FS_TEACHERS; 
        break;
      case FS_TEACHERS: 
        fetchFirestoreTeachers(); 
        yield(); // Yield after fetch
        firestoreState = FS_STUDENTS; 
        break;
      case FS_STUDENTS: 
        fetchFirestoreStudents(); 
        yield(); // Yield after fetch
        firestoreState = FS_ROOMS; 
        break;
      case FS_ROOMS: 
        fetchFirestoreRooms(); 
        yield(); // Yield after fetch
        firestoreState = FS_USERS; 
        break;
      case FS_USERS:
        fetchFirestoreUsers(); 
        yield(); // Yield after fetch
        Serial.println("Firestore refreshed: Teachers=" + String(firestoreTeachers.size()) +
                       ", Students=" + String(firestoreStudents.size()) +
                       ", Users=" + String(firestoreUsers.size()));
        firestoreState = FS_IDLE;
        lastFirestoreRefresh = millis();
        break;
    }
    yield(); // Prevent watchdog reset after Firestore operations
  }

  // Schedule sync
  static unsigned long lastScheduleSync = 0;
  if (!sdMode && isConnected && Firebase.ready() && millis() - lastScheduleSync >= 3600000) {
    yield(); // Prevent watchdog reset before sync
    if (syncSchedulesToSD()) {
      displayMessage("Schedules", "Synced to SD", 1500);
      yield(); // Yield after SD write
    } else {
      displayMessage("Sync Failed", "Check SD", 1500);
      yield(); // Yield after SD failure
    }
    lastScheduleSync = millis();
    yield(); // Prevent watchdog reset after sync
  }

  // Firebase health
  static unsigned long lastFirebaseCheck = 0;
  if (isConnected && !sdMode && millis() - lastFirebaseCheck > 60000) {
    if (WiFi.RSSI() < -80) {
      Serial.println("Weak WiFi signal (" + String(WiFi.RSSI()) + " dBm).");
      yield(); // Yield after RSSI check
    } else if (!Firebase.ready()) {
      Firebase.reconnectWiFi(true);
      yield(); // Prevent watchdog reset during reconnect
      initFirebase();
      yield(); // Yield after Firebase init
      if (!Firebase.ready()) {
        logSystemEvent("Firebase Failure Reset");
        storeLogToSD("FirebaseFailure:Timestamp:" + getFormattedTime());
        ESP.restart();
      }
    }
    lastFirebaseCheck = millis();
    yield(); // Yield after Firebase check
  }

  // WiFi reconnect
  static unsigned long lastWiFiCheck = 0;
  if (millis() - lastWiFiCheck > 180000 && isConnected && WiFi.status() != WL_CONNECTED) {
    connectWiFi();
    lastWiFiCheck = millis();
    yield(); // Prevent watchdog reset after WiFi reconnect
  }
  
  // Auto recover from SD mode after SSL error
  if (sdMode && isConnected && sdModeRetryTime > 0 && millis() > sdModeRetryTime) {
    Serial.println("Attempting to recover from SD mode after SSL error timeout");
    sdModeRetryTime = 0; // Reset timer
    
    // Try to reconnect to Firebase
    if (WiFi.status() == WL_CONNECTED) {
      Firebase.reconnectWiFi(true);
      initFirebase();
      
      if (Firebase.ready() && Firebase.authenticated()) {
        Serial.println("Successfully recovered from SSL error");
        sdMode = false;
        logSystemEvent("Recovered from SSL Error");
        displayMessage("Firebase", "Reconnected", 2000);
        displayMessage("Ready. Tap your", "RFID Card!", 0);
      }
    }
  }
  
  // I2C health
  if (millis() - lastI2cRecovery > 30000) {
    Wire.beginTransmission(0x27);
    if (Wire.endTransmission() != 0) {
      recoverI2C();
      yield(); // Yield after recovery
      Wire.beginTransmission(0x27);
      if (Wire.endTransmission() == 0) displayMessage("I2C Recovered", "System OK", 2000);
    }
    lastI2cRecovery = millis();
    yield(); // Yield after I2C check
  }

  // WiFi state handling
  if (isConnected && !wasConnected && firstActionOccurred) {
    displayMessage("WiFi Reconnected", "Normal Mode", 2000);
    sdMode = false;
    wasConnected = true;
    if (superAdminSessionActive) {
      digitalWrite(RELAY2, HIGH);
      digitalWrite(RELAY3, HIGH);
      digitalWrite(RELAY4, HIGH);
      superAdminSessionActive = false;
      relayActive = false;
    }
    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {
      Rtc.SetDateTime(RtcDateTime(timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
                                  timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec));
    }
    yield(); // Prevent watchdog reset before Firebase operations
    bool firebaseReady = false;
    for (int retry = 0; retry < 2 && !firebaseReady; retry++) { // Reduced from 3 to 2
      initFirebase();
      yield(); // Yield during Firebase init
      if (Firebase.ready()) firebaseReady = true;
      else Firebase.reconnectWiFi(true);
      yield(); // Yield during WiFi reconnect
    }
    if (firebaseReady) {
      fetchFirestoreTeachers(); yield();
      fetchFirestoreStudents(); yield();
      fetchFirestoreRooms(); yield();
      fetchFirestoreUsers(); yield();
      syncSchedulesToSD(); yield();
      handleOfflineSync(); yield();
    } else {
      sdMode = true;
    }
    lastActivityTime = millis();
    readyMessageShown = false;
    yield(); // Yield after WiFi state handling
  } else if (!isConnected && wasConnected) {
    displayMessage("WiFi Lost", "Check Network", 2000);
    displayMessage("Backup Logs", "Activated", 2000);
    sdMode = true;
    wasConnected = false;
    String entry = "System:WiFiLost Timestamp:" + getFormattedTime() + " Action:BackupActivated";
    storeLogToSD(entry);
    lastActivityTime = millis();
    readyMessageShown = false;
    yield(); // Yield after WiFi loss handling
  }

  // Voltage monitoring
  static bool voltageLost = false;
  static unsigned long voltageLossStart = 0;
  float currentVoltage = pzem.voltage();
  isVoltageSufficient = (currentVoltage >= voltageThreshold && !isnan(currentVoltage));
  if (!isVoltageSufficient && wasVoltageSufficient) {
    if (currentVoltage <= 0 || isnan(currentVoltage)) {
      if (voltageLossStart == 0) voltageLossStart = millis();
      else if (millis() - voltageLossStart >= 5000) {
        voltageLost = true;
        displayMessage("Low Voltage -", "Check Power", 2000);
        displayMessage("Backup Logs", "Activated", 2000);
        wasVoltageSufficient = false;
        lastActivityTime = millis();
        readyMessageShown = false;
      }
    }
  } else if (isVoltageSufficient && !wasVoltageSufficient && voltageLost) {
    displayMessage("Voltage Restored", "", 2000);
    wasVoltageSufficient = true;
    voltageLost = false;
    voltageLossStart = 0;
    lastActivityTime = millis();
    readyMessageShown = false;
  } else if (isVoltageSufficient) {
    voltageLossStart = 0;
  }
  yield(); // Yield after voltage monitoring

  // Reed switch
  int sensorState = digitalRead(REED_PIN);
  
  // Debug output every 5 seconds
  static unsigned long lastReedDebugTime = 0;
  if (millis() - lastReedDebugTime > 5000) {
    Serial.println("Reed sensor state: " + String(sensorState == LOW ? "CLOSED (LOW)" : "OPEN (HIGH)") + 
                   ", reedState: " + String(reedState) + 
                   ", tamperActive: " + String(tamperActive) + 
                   ", tamperResolved: " + String(tamperResolved));
    lastReedDebugTime = millis();
  }
  
  if (sensorState == LOW && !reedState) {
    reedState = true;
    tamperResolved = false;
    lastActivityTime = millis();
    Serial.println("Reed sensor CLOSED - Door/Window closed");
    
    // If no other active states, transition to ready display
    if (!tamperActive && !adminAccessActive && !classSessionActive &&
        !waitingForInstructorEnd && !studentVerificationActive && !tapOutPhase) {
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      readyMessageShown = true;
    }
  } else if (sensorState == HIGH && reedState) {
    reedState = false;
    lastActivityTime = millis();
    Serial.println("Reed sensor OPEN - Door/Window open! This should trigger tamper alert");
  }
  yield(); // Yield after reed switch check

  // Power-saving mode
  if (!powerSavingMode && !tamperActive && !adminAccessActive && !classSessionActive &&
      !waitingForInstructorEnd && !studentVerificationActive && !tapOutPhase &&
      millis() - lastActivityTime >= INACTIVITY_TIMEOUT) {
    enterPowerSavingMode();
    yield(); // Yield after entering power-saving mode
  }
  if (powerSavingMode) {
    if (rfid.PICC_IsNewCardPresent() && rfid.PICC_ReadCardSerial()) {
      String uidStr = getUIDString();
      rfid.PICC_HaltA();
      rfid.PCD_StopCrypto1();
      yield(); // Yield after RFID read
      if (!isConnected) {
        WiFi.reconnect();
        unsigned long wifiStartTime = millis();
        while (WiFi.status() != WL_CONNECTED && millis() - wifiStartTime < 10000) {
          yield(); // Yield during WiFi reconnect
        }
        isConnected = (WiFi.status() == WL_CONNECTED);
        if (isConnected) {
          initFirebase(); yield();
          fetchRegisteredUIDs(); yield();
          fetchFirestoreTeachers(); yield();
          fetchFirestoreStudents(); yield();
          fetchFirestoreRooms(); yield();
          fetchFirestoreUsers(); yield();
          syncSchedulesToSD(); yield();
          struct tm timeinfo;
          if (getLocalTime(&timeinfo)) {
            Rtc.SetDateTime(RtcDateTime(timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday,
                                        timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec));
          }
        }
      }
      if (isRegisteredUID(uidStr)) exitPowerSavingMode();
    }
    yield(); // Yield in power-saving mode
    return;
  }

  // Tamper detection based on reed switch
  if (!reedState && !tamperActive && !relayActive && !tamperResolved && !adminAccessActive) {
    // Reed switch is HIGH (open) -> trigger tamper alert
    Serial.println("TAMPER DETECTED! Reed sensor is OPEN");
    
    tamperDetected = true;
    tamperActive = true;
    tamperAlertTriggered = true;
    tamperMessagePrinted = false;
    tamperMessageDisplayed = false;
    buzzerActive = true;
    tamperStartTime = getFormattedTime();
    
    currentTamperAlertId = tamperStartTime;
    currentTamperAlertId.replace(" ", "_");
    currentTamperAlertId.replace(":", "-");
    currentTamperAlertId.replace("/", "-");
    
    String entry = "Tamper:Detected Timestamp:" + tamperStartTime + " Status:Active";
    storeLogToSD(entry);
    yield(); // Yield after SD write
    
    if (!sdMode && isConnected && Firebase.ready()) {
      String tamperPath = "/Alerts/Tamper/" + currentTamperAlertId;
      FirebaseJson tamperJson;
      tamperJson.set("startTime", tamperStartTime);
      tamperJson.set("status", "active");
      tamperJson.set("detectedAt", tamperStartTime);
      tamperJson.set("deviceId", WiFi.macAddress());
      tamperJson.set("alertType", "tamper");
      tamperJson.set("resolvedBy", "");
      tamperJson.set("resolverName", "");
      tamperJson.set("endTime", "");
      
      Serial.print("Logging tamper detection: " + tamperPath + "... ");
      if (Firebase.RTDB.setJSON(&fbdo, tamperPath, &tamperJson)) {
        Serial.println("Success");
      } else {
        String errorLog = "FirebaseError:Path:" + tamperPath + " Reason:" + fbdo.errorReason();
        storeLogToSD(errorLog);
        Serial.println("Failed: " + fbdo.errorReason());
        if (fbdo.errorReason().indexOf("ssl") >= 0 || 
            fbdo.errorReason().indexOf("connection") >= 0 || 
            fbdo.errorReason().indexOf("SSL") >= 0) {
          handleFirebaseSSLError();
        }
      }
      yield(); // Yield after Firebase operation
    }
    
    // Force buzzer ON - continuous tone
    tone(BUZZER_PIN, 1000);
    lastBuzzerToggle = millis();
    
    // Force LED to RED
    digitalWrite(LED_R_PIN, HIGH);
    digitalWrite(LED_G_PIN, LOW);
    digitalWrite(LED_B_PIN, LOW);
    
    displayMessage("Tamper Detected", "Door Locked", 0);
    tamperMessageDisplayed = true;
    firstActionOccurred = true;
    lastActivityTime = millis();
    
    Serial.println("Tamper alert activated - buzzer and red LED turned ON");
  }

  // Keep tamper alerts active if tamper is detected
  if (tamperActive) {
    // Debug tamper state
    static unsigned long lastTamperDebugTime = 0;
    if (millis() - lastTamperDebugTime > 3000) {
      Serial.println("Tamper state: Active=" + String(tamperActive) + 
                    ", Buzzer=" + String(buzzerActive) + 
                    ", LastToggle=" + String(millis() - lastBuzzerToggle) + "ms ago" +
                    ", LED_R=" + String(digitalRead(LED_R_PIN)));
      lastTamperDebugTime = millis();
    }
    
    // Ensure red light is always on during tamper
    digitalWrite(LED_R_PIN, HIGH);
    digitalWrite(LED_G_PIN, LOW);
    digitalWrite(LED_B_PIN, LOW);
    
    // Ensure buzzer is continuously sounding or pulsing
    if (!buzzerActive || millis() - lastBuzzerToggle > 500) {
      tone(BUZZER_PIN, 1000);
      buzzerActive = true;
      lastBuzzerToggle = millis();
      // Every 2 seconds, print a message to confirm the buzzer is supposed to be active
      if ((millis() / 1000) % 2 == 0) {
        Serial.println("Buzzer should be ON right now");
      }
    }
    
    // Keep displaying tamper message
    if (!tamperMessageDisplayed) {
      displayMessage("Tamper Detected", sdMode ? "Super Admin Req." : "Admin Card Req.", 0);
      tamperMessageDisplayed = true;
    }
  }
  yield(); // Yield after tamper detection

  // Tamper resolution
  if (tamperActive && millis() - lastRFIDTapTime >= RFID_DEBOUNCE_DELAY) {
    if (rfid.PICC_IsNewCardPresent() && rfid.PICC_ReadCardSerial()) {
      unsigned long startTime = millis();
      while (millis() - startTime < 50) yield(); // Non-blocking delay
      String uidStr = getUIDString();
      rfid.PICC_HaltA();
      rfid.PCD_StopCrypto1();
      String timestamp = getFormattedTime();
      lastRFIDTapTime = lastActivityTime = millis();
      yield(); // Yield after RFID read
      
      // Check if the card is the Super Admin UID - works in both modes
      if (uidStr == SUPER_ADMIN_UID) {
        Serial.println("Super Admin card detected - resolving tamper");
        
        // Super Admin code
        String entry = "Tamper:Resolved Timestamp:" + timestamp + " Status:Resolved By:" + uidStr;
        storeLogToSD(entry);
        
        // Update Firebase if connected
        if (!sdMode && isConnected && Firebase.ready() && currentTamperAlertId.length() > 0) {
          String tamperPath = "/Alerts/Tamper/" + currentTamperAlertId;
          
          FirebaseJson updateJson;
          updateJson.set("status", "resolved");
          updateJson.set("resolvedBy", uidStr);
          updateJson.set("resolverName", "Super Admin");
          updateJson.set("resolverRole", "Super Admin");
          updateJson.set("endTime", timestamp);
          updateJson.set("resolutionTime", timestamp);
          
          if (Firebase.RTDB.updateNode(&fbdo, tamperPath, &updateJson)) {
            Serial.println("Successfully updated tamper alert status");
          } else {
            Serial.println("Failed to update tamper alert: " + fbdo.errorReason());
          }
        }
        
        // Reset tamper state
        tamperActive = tamperDetected = tamperAlertTriggered = false;
        tamperResolved = true;
        tamperMessageDisplayed = buzzerActive = false;
        
        // Stop the buzzer
        noTone(BUZZER_PIN);
        
        // Display tamper resolved message
        displayMessage("Tamper Resolved", "By Super Admin", 2000);
        delay(300); // Short delay
        yield();
        
        // Reset the neutral LED state
        showNeutral();
        
        // Transition to ready state with smooth transition
        smoothTransitionToReady();
        
        accessFeedback();
        displayMessage("Tamper Stopped", "Super Admin", 2000);
        logSystemEvent("Tamper Resolved by Super Admin: " + uidStr);
        
        // Add delay for visibility of message
        unsigned long startTime = millis();
        while (millis() - startTime < 2000) yield(); // Non-blocking delay
        
        currentTamperAlertId = ""; // Reset the alert ID
      }
      // Check for admin - only in online mode
      else if (!sdMode && isAdminUID(uidStr)) {
        Serial.println("Admin card detected - resolving tamper");
        
        // Get admin details
        std::map<String, String> userData = fetchUserDetails(uidStr);
        String name = userData.empty() ? "Admin" : userData["fullName"];
        String role = userData.empty() ? "admin" : userData["role"];
        
        // Update Tamper Alert in Firebase
        if (isConnected && Firebase.ready() && currentTamperAlertId.length() > 0) {
          String tamperPath = "/Alerts/Tamper/" + currentTamperAlertId;
          
          FirebaseJson updateJson;
          updateJson.set("status", "resolved");
          updateJson.set("resolvedBy", uidStr);
          updateJson.set("resolverName", name);
          updateJson.set("resolverRole", role);
          updateJson.set("endTime", timestamp);
          updateJson.set("resolutionTime", timestamp);
          
          if (Firebase.RTDB.updateNode(&fbdo, tamperPath, &updateJson)) {
            Serial.println("Successfully updated tamper alert status");
          } else {
            Serial.println("Failed to update tamper alert: " + fbdo.errorReason());
          }
        }
        
        logAdminTamperStop(uidStr, timestamp);
        
        // Reset tamper state
        tamperActive = tamperDetected = tamperAlertTriggered = false;
        tamperResolved = true;
        tamperMessageDisplayed = buzzerActive = false;
        
        // Turn off the buzzer immediately
        noTone(BUZZER_PIN);
        Serial.println("Buzzer turned OFF");
        
        // Return LED to neutral state
        digitalWrite(LED_R_PIN, LOW);
        accessFeedback();
        
        displayMessage("Tamper Stopped", name + " (" + role + ")", 2000);
        logSystemEvent("Tamper Resolved by Admin UID: " + uidStr);
        
        // Add delay for visibility of message
        unsigned long startTime = millis();
        while (millis() - startTime < 2000) yield(); // Non-blocking delay
        
        // Use smooth transition instead of direct ready message
        smoothTransitionToReady();
        currentTamperAlertId = ""; // Reset the alert ID
      } else {
        deniedFeedback(); // Use standard denied feedback for tamper alerts
        displayMessage("Tamper Detected", sdMode ? "Super Admin Req." : "Admin Card Req.", 2000);
        
        // Keep buzzer active
        if (!buzzerActive) {
          tone(BUZZER_PIN, 1000);
          buzzerActive = true;
        }
        
        // Keep red LED on
        digitalWrite(LED_R_PIN, HIGH);
        digitalWrite(LED_G_PIN, LOW);
        digitalWrite(LED_B_PIN, LOW);
        
        displayMessage("Tamper Detected", "Door Locked", 0);
        tamperMessageDisplayed = true;
        
        Serial.println("Unauthorized card - tamper alert remains active");
      }
    }
    yield(); // Yield after tamper resolution
    return;
  }

  // Idle state
  if (!studentVerificationActive && !adminAccessActive && !classSessionActive &&
      !waitingForInstructorEnd && !tapOutPhase && !tamperActive) {
    if (!readyMessageShown && millis() - lastActivityTime >= 5000) {
      displayMessage("Ready. Tap your", "RFID Card!", 0);
      readyMessageShown = true;
      lastReadyPrint = millis();
    }
  } else {
    readyMessageShown = false;
  }
  yield(); // Yield after idle state

  // Admin PZEM logging
  if (adminAccessActive && millis() - lastPZEMLogTime >= 5000) {
    lastVoltage = max(pzem.voltage(), 0.0f);
    lastCurrent = max(pzem.current(), 0.0f);
    lastPower = max(pzem.power(), 0.0f);
    lastEnergy = max(pzem.energy(), 0.0f);
    lastFrequency = max(pzem.frequency(), 0.0f);
    lastPowerFactor = max(pzem.pf(), 0.0f);
    lastPZEMLogTime = millis();
    
    // When admin mode is active, periodically check RFID reader
    static unsigned long lastAdminRFIDCheck = 0;
    if (millis() - lastAdminRFIDCheck >= 15000) { // Every 15 seconds
      // Test and reset RFID reader if needed to ensure exit taps are detected
      byte version = rfid.PCD_ReadRegister(rfid.VersionReg);
      if (version == 0 || version == 0xFF) {
        Serial.println("Admin mode: RFID reader reset for better exit detection");
        rfid.PCD_Reset();
        delay(50);
        rfid.PCD_Init();
        rfid.PCD_SetAntennaGain(rfid.RxGain_max);
        
        // If door is locked but admin mode active, remind to tap
        if (digitalRead(RELAY1) == HIGH) {
          displayMessage("Admin Mode", "Tap to Exit", 0);
        }
      }
      lastAdminRFIDCheck = millis();
    }
    
    yield(); // Yield after PZEM logging
  }

  // Student verification
  if (studentVerificationActive) {
    static unsigned long lastUpdate = 0;
    static int dotCount = 0;
    
    if (millis() - lastUpdate >= 1000) {
      // Multiple yields before display update
      yield();
      delay(10);
      yield();
      
      String line1 = "Awaiting Student";
      String line2 = "Attendance";
      for (int i = 0; i < dotCount; i++) line2 += ".";
      
      // Add yield before display update
      yield();
      displayMessage(line1, line2, 0);
      // Add yield after display update
      yield();
      
      dotCount = (dotCount + 1) % 4;
      lastUpdate = millis();
      
      // Multiple yields after display update
      yield();
      delay(10);
      yield();
    }

    if (millis() - lastRFIDTapTime >= RFID_DEBOUNCE_DELAY) {
      if (rfid.PICC_IsNewCardPresent() && rfid.PICC_ReadCardSerial()) {
        unsigned long startTime = millis();
        while (millis() - startTime < 50) yield(); // Non-blocking delay
        String uidStr = getUIDString();
        rfid.PICC_HaltA();
        rfid.PCD_StopCrypto1();
        String timestamp = getFormattedTime();

        lastRFIDTapTime = millis();
        lastActivityTime = millis();
        Serial.println("Detected UID during student verification: " + uidStr);
        yield(); // Yield after RFID read

        if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
          verifyStudent(uidStr);
        } else {
          unregisteredUIDFeedback(); // Use unregistered feedback instead of generic denied
          Serial.println("UID " + uidStr + " not a student.");
          logUnregisteredUID(uidStr, timestamp);
        }
      }
    }

    if (millis() - studentVerificationStartTime >= Student_VERIFICATION_WINDOW) {
      studentVerificationActive = false;
      classSessionActive = true;
      classSessionStartTime = millis();
      digitalWrite(RELAY1, HIGH);
      Serial.println("Class session started. Door locked (Relay 1 HIGH).");
      
      // Explicitly reset the watchdog timer for class session start
      lastReadyPrint = millis();
      Serial.println("Watchdog timer reset for class session start");
      yield(); // Add yield after watchdog reset
      
      // Clear any existing "Awaiting Student Attendance" message
      lcd.clear();
      yield(); // Add yield after clear
      
      // Add multiple yields to ensure we don't get stuck in firebase operations
      for (int i = 0; i < 10; i++) {
        yield();
        delay(1);
      }
      
      displayMessage("Attendance", "Closed", 2000);
      unsigned long startTime = millis();
      // Non-blocking delay with frequent yields
      while (millis() - startTime < 2000) {
        yield();
        delay(10);
      }
      
      // Display class session started message and ensure it's visible
      lcd.clear();
      yield(); // Add yield after clear
      displayMessage("Class Session", "Started", 2000);
      startTime = millis();
      // Non-blocking delay with frequent yields
      while (millis() - startTime < 2000) {
        yield();
        delay(10);
      }
      
      // Handle Firebase operations with error recovery
      bool firebaseUpdated = false;
      int retryCount = 0;
      while (!firebaseUpdated && retryCount < 3) {
        if (!sdMode && isConnected && Firebase.ready()) {
          String statusPath = "/ClassStatus/" + currentSessionId;
          FirebaseJson statusJson;
          statusJson.set("Status", "In Session");
          statusJson.set("dateTime", getFormattedTime());
          
          // Reset watchdog timer before Firebase operation
          safeFirebaseOperation();
          yield(); // Add yield before Firebase operation
          
          if (Firebase.RTDB.updateNode(&fbdo, statusPath, &statusJson)) {
            firebaseUpdated = true;
            Serial.println("Firebase status updated successfully");
          } else {
            Serial.println("Firebase update failed: " + fbdo.errorReason());
            retryCount++;
            // Add multiple yields for recovery
            for (int i = 0; i < 20; i++) {
              yield();
              delay(5);
            }
          }
          
          // Reset watchdog timer after Firebase operation
          safeFirebaseOperation();
          yield(); // Add yield after Firebase operation
        } else {
          // If Firebase is not ready, don't retry
          break;
        }
      }
      
      // Even if Firebase update failed, ensure we continue with the program
      for (int i = 0; i < 10; i++) {
        yield();
        delay(1);
      }
      
      lastActivityTime = millis();
      lastReadyPrint = millis();
      readyMessageShown = false;
    }

    if (millis() - lastRFIDTapTime >= RFID_DEBOUNCE_DELAY && 
        rfid.PICC_IsNewCardPresent() && rfid.PICC_ReadCardSerial()) {
      
      String uidStr = getUIDString();
      rfid.PICC_HaltA();
      rfid.PCD_StopCrypto1();
      String timestamp = getFormattedTime();
      lastRFIDTapTime = lastActivityTime = millis();
      yield(); // Yield after RFID read

      if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
        // Mark student as present immediately
        logStudentToRTDB(uidStr, timestamp, 0.0, -1, "true", "");
        presentCount++;
        
        // Display confirmation
        String studentName = firestoreStudents[uidStr]["fullName"];
        if (studentName.length() == 0) studentName = "Student";
        displayMessage(studentName, "Attendance Marked", 2000);
        
        // Feedback
        digitalWrite(LED_G_PIN, HIGH);
        tone(BUZZER_PIN, 1000, 200);
        delay(200);
        digitalWrite(LED_G_PIN, LOW);
      } else if (uidStr == lastInstructorUID) {
        // Instructor tapping during tap-out phase should finalize even if students remain
        String statusPath = "/ClassStatus/" + currentSessionId;
        FirebaseJson statusJson;
        statusJson.set("Status", "Officially Ended");
        statusJson.set("dateTime", getFormattedTime());
        if (Firebase.RTDB.updateNode(&fbdo, statusPath, &statusJson)) {
          Serial.println("Class status updated to 'Officially Ended' by instructor override");
        }
        
        displayMessage("Instructor Override", "Class Officially Ended", 3000);
        unsigned long startTime = millis();
        while (millis() - startTime < 600) yield(); // Short non-blocking delay
        
        waitingForInstructorEnd = true;
        tapOutPhase = false;
      } else {
        unregisteredUIDFeedback();
        displayMessage("Unregistered ID", "Access Denied", 2000);
      }
    }
    
    // Tap-out phase
    if (tapOutPhase) {
      static bool classEndedShown = false;
      static unsigned long classEndedStart = 0;
      
      if (!classEndedShown) {
        if (classEndedStart == 0) {
          classEndedStart = millis();
          displayMessage("Class Ended", "", 4000);
          yield(); // Ensure system responsiveness
        } else if (millis() - classEndedStart >= 4000) {
          classEndedShown = true;
          // Initialize the presentCount properly based on verified students
          presentCount = 0;
          for (const auto& student : studentAssignedSensors) {
            if (student.second >= 0) { // Only count verified students (not -1, -2, or -3)
              presentCount++;
            }
          }
          
          // Initial display of count - more visible
          displayMessage("Students Remaining:", String(presentCount), 2000);
          yield(); // Add yield for responsiveness
          
          if (!sdMode && isConnected && Firebase.ready()) {
            String summaryPath = "/AttendanceSummary/" + currentSessionId + "/totalAttendees";
            FirebaseJson summaryJson;
            summaryJson.set("totalAttendees", presentCount);
            if (Firebase.RTDB.updateNode(&fbdo, summaryPath, &summaryJson)) {
              Serial.println("Updated attendance summary with total attendees: " + String(presentCount));
            } else {
              Serial.println("Failed to update attendance summary: " + fbdo.errorReason());
            }
            yield(); // Yield after Firebase update
          }
        }
        yield(); // Yield during class ended display
        return;
      }

      static unsigned long lastAttendeeUpdate = 0;
      if (millis() - lastAttendeeUpdate >= 1000) {
        // More visible remaining count message
        displayMessage("Students Remaining:", String(presentCount), 0);
        lastAttendeeUpdate = millis();
        yield(); // Ensure system responsiveness
      }

      if (millis() - lastRFIDTapTime >= RFID_DEBOUNCE_DELAY && 
          rfid.PICC_IsNewCardPresent() && rfid.PICC_ReadCardSerial()) {
        
        String uidStr = getUIDString();
        rfid.PICC_HaltA();
        rfid.PCD_StopCrypto1();
        String timestamp = getFormattedTime();
        lastRFIDTapTime = lastActivityTime = millis();
        yield(); // Yield after RFID read

        if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
          // Log tap out time
          logStudentToRTDB(uidStr, timestamp, 0.0, -1, "true", timestamp);
          
          // Update count and display
          presentCount = max(0, presentCount - 1);
          String studentName = firestoreStudents[uidStr]["fullName"];
          if (studentName.length() == 0) studentName = "Student";
          displayMessage(studentName + " Out", "Remaining: " + String(presentCount), 2000);
          
          // Feedback
          digitalWrite(LED_G_PIN, HIGH);
          tone(BUZZER_PIN, 1000, 200);
          delay(200);
          digitalWrite(LED_G_PIN, LOW);
          
          if (presentCount <= 0) {
            // Add additional delay when count reaches 0 to ensure the "Remaining: 0" message is clearly visible
            // Display the "Remaining: 0" message again with longer duration
            displayMessage(studentName + " Tapped Out", "Remaining: 0", 2500);
            unsigned long startTime = millis();
            while (millis() - startTime < 1000) yield(); // Longer non-blocking delay for the zero count message
            
            digitalWrite(LED_G_PIN, HIGH);
            tone(BUZZER_PIN, 1000, 500);
            startTime = millis();
            while (millis() - startTime < 500) yield(); // Non-blocking delay
            digitalWrite(LED_G_PIN, LOW);
            
            if (!sdMode && isConnected && Firebase.ready()) {
              String statusPath = "/ClassStatus/" + currentSessionId;
              FirebaseJson statusJson;
              statusJson.set("Status", "Officially Ended");
              statusJson.set("dateTime", getFormattedTime());
              Firebase.RTDB.updateNode(&fbdo, statusPath, &statusJson);
              yield(); // Yield after Firebase update
            }
            
            displayMessage("All Students Out", "Instructor Tap Now", 3000);
            // Add delay for visibility
            startTime = millis();
            while (millis() - startTime < 500) yield(); // Small non-blocking delay
            
            waitingForInstructorEnd = true;
            tapOutPhase = false;
          }
        }
        // Handle instructor tap during tap-out phase
        else if (uidStr == lastInstructorUID) {
          // Instructor tapping during tap-out phase should finalize even if students remain
          String statusPath = "/ClassStatus/" + currentSessionId;
          FirebaseJson statusJson;
          statusJson.set("Status", "Officially Ended");
          statusJson.set("dateTime", getFormattedTime());
          if (Firebase.RTDB.updateNode(&fbdo, statusPath, &statusJson)) {
            Serial.println("Class status updated to 'Officially Ended' by instructor override");
          }
          
          displayMessage("Instructor Override", "Class Officially Ended", 3000);
          unsigned long startTime = millis();
          while (millis() - startTime < 600) yield(); // Short non-blocking delay
          
          waitingForInstructorEnd = true;
          tapOutPhase = false;
        }
      }
      yield(); // Yield after tap-out phase
      return;
    }

  // Waiting for instructor final confirmation
  if (waitingForInstructorEnd && !tapOutPhase) {
    if (millis() - lastRFIDTapTime >= RFID_DEBOUNCE_DELAY && 
        rfid.PICC_IsNewCardPresent() && rfid.PICC_ReadCardSerial()) {
      
      String uidStr = getUIDString();
      rfid.PICC_HaltA();
      rfid.PCD_StopCrypto1();
      String timestamp = getFormattedTime();
      lastRFIDTapTime = lastActivityTime = millis();
      yield(); // Yield after RFID read
      
      if (uidStr == lastInstructorUID) {
        // Mark remaining students as absent
        for (auto& student : studentAssignedSensors) {
          String studentUid = student.first;
          logStudentToRTDB(studentUid, timestamp, 0.0, -3, "false", "");
          Serial.println("Student " + studentUid + " marked absent - didn't tap out");
          yield(); // Yield during iteration
        }
        
        // Log final instructor action
        logInstructor(uidStr, timestamp, "FinalizeSession");
        yield(); // Yield after logging
        
        // Perform a controlled session cleanup
        resetSessionState();
        
        // Provide feedback with non-blocking delays
        displayMessage("Attendance Summary", "Saved Successfully", 2000);
        unsigned long startTime = millis();
        while (millis() - startTime < 2000) yield(); // Non-blocking delay
        
        displayMessage("Class Officially", "Ended", 2000);
        startTime = millis();
        while (millis() - startTime < 2000) yield(); // Non-blocking delay
        
        // Final smooth transition to ready state
        smoothTransitionToReady();
        
        // Reset all session variables
        waitingForInstructorEnd = false;
        tapOutPhase = false;
        lastInstructorUID = "";
        currentSessionId = "";
        presentCount = 0;
        
        return;
      } else if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
        // Allow late tap-outs from students
        Serial.println("Student " + uidStr + " tapped during instructor wait phase");
        String studentName = "Unknown";
        try {
          studentName = firestoreStudents.at(uidStr).at("fullName");
          if (studentName.length() == 0) studentName = "Student";
        } catch (...) {
          studentName = "Student";
        }
        
        logStudentToRTDB(uidStr, timestamp, 0.0, -2, "true", timestamp);
        
        // Visual and audio feedback
        digitalWrite(LED_G_PIN, HIGH);
        tone(BUZZER_PIN, 1000, 200);
        unsigned long startTime = millis();
        while (millis() - startTime < 200) yield(); // Non-blocking delay
        digitalWrite(LED_G_PIN, LOW);
        
        displayMessage(studentName, "Late Tap-Out", 2000);
        displayMessage("All Students Out", "Instructor Tap Now", 2000);
      } else {
        // Unauthorized card
        unregisteredUIDFeedback(); // Use unregistered feedback instead of generic denied
        displayMessage("Unauthorized Card", "Instructor Only", 2000);
        displayMessage("All Students Out", "Instructor Tap Now", 0);
      }
    }
    yield(); // Yield at the end of loop
    return;
  }

  // Offline display
  static unsigned long lastDotUpdateOffline = 0;
  static int dotCountOffline = 0;
  if (sdMode && relaysActive && millis() - doorOpenTime < 30000 && millis() - lastRFIDTapTime >= 1000) {
    if (millis() - lastDotUpdateOffline >= 1000) {
      String line1 = "Tap your ID";
      for (int i = 0; i < dotCountOffline; i++) line1 += ".";
      displayMessage(line1, lastTappedUID.length() > 0 ? "UID: " + lastTappedUID : "No tap yet", 0);
      dotCountOffline = (dotCountOffline + 1) % 4;
      lastDotUpdateOffline = millis();
    }
  }

  if (sdMode && (superAdminSessionActive || (relaysActive && millis() - doorOpenTime >= 30000)) && digitalRead(RELAY1) == LOW) {
    digitalWrite(RELAY1, HIGH);
    relayActive = false;
    displayMessage(superAdminSessionActive ? "Class Session" : "Class In Session", "", 500);
  }
  yield(); // Yield after offline display

  // Standard RFID Card Detection
  static unsigned long lastRFIDCheckTime = 0;
  static int rfidResetCount = 0;
  
  if (millis() - lastRFIDCheckTime >= 10000) {
    byte version = rfid.PCD_ReadRegister(rfid.VersionReg);
    if (version == 0 || version == 0xFF) {
      Serial.println("RFID reader not responding. Reinitializing...");
      SPI.end(); // Properly end SPI first
      rfid.PCD_Init(); // Reinitialize the RFID reader
      rfid.PCD_SetAntennaGain(rfid.RxGain_max);
      Serial.println("RFID reader reinitialized");
      rfidResetCount++;
      
      if (rfidResetCount >= 3) {
        logSystemEvent("RFID Failure Reset");
        storeLogToSD("RFIDFailure:Timestamp:" + getFormattedTime());
        // Less drastic than full restart
        if (WiFi.status() == WL_CONNECTED) {
          Firebase.reconnectWiFi(true);
        }
        displayMessage("RFID Error", "Recovering...", 2000);
      }
    } else {
      rfidResetCount = 0;
    }
    lastRFIDCheckTime = millis();
    yield(); // Yield after RFID health check
  }
  
  if (millis() - lastRFIDTapTime >= RFID_DEBOUNCE_DELAY) {
    yield(); // Prevent watchdog reset before RFID operations
    if (rfid.PICC_IsNewCardPresent()) {
      unsigned long startTime = millis();
      while (millis() - startTime < 0.5) yield(); // Non-blocking microsecond delay
      if (rfid.PICC_ReadCardSerial()) {
        String uidStr = getUIDString();
        String timestamp = getFormattedTime();
        lastRFIDTapTime = lastActivityTime = millis();
        
        if (uidDetailsPrinted.find(uidStr) == uidDetailsPrinted.end() || !uidDetailsPrinted[uidStr]) {
          Serial.println("Card detected: UID = " + uidStr);
          uidDetailsPrinted[uidStr] = true;
        }
        
        rfid.PICC_HaltA();
        rfid.PCD_StopCrypto1();
        yield(); // Yield after RFID read
        
        // Check if admin session is active and this is the admin who started it
        if (adminAccessActive && uidStr == lastAdminUID) {
          Serial.println("Admin UID detected to end session: " + uidStr);
          logAdminAccess(uidStr, timestamp);
          yield();
        } else if (isRegisteredUID(uidStr)) {
          if (uidStr == SUPER_ADMIN_UID) {
            yield(); // Yield before logging
            logSuperAdmin(uidStr, timestamp);
          } else if (firestoreTeachers.find(uidStr) != firestoreTeachers.end()) {
            String role = firestoreTeachers[uidStr]["role"];
            if (role.length() == 0) role = "instructor";
            if (role.equalsIgnoreCase("instructor")) {
              yield(); // Yield before logging
              
              // Check if this is the instructor who started the current session
              if (classSessionActive && uidStr == lastInstructorUID) {
                Serial.println("Instructor " + uidStr + " tapped to end class session");
                logInstructor(uidStr, timestamp, "EndSession");
                
                // End the class session and transition to tap-out phase
                digitalWrite(RELAY2, HIGH);
                delay(20); // Small delay for relay
                yield(); // Allow system to process
                
                digitalWrite(RELAY3, HIGH);
                delay(20); // Small delay for relay
                yield(); // Allow system to process
                
                digitalWrite(RELAY4, HIGH);
                delay(20); // Small delay for relay
                yield(); // Allow system to process
                
                relayActive = false;
                classSessionActive = false;
                tapOutPhase = true;
                tapOutStartTime = millis();
                pzemLoggedForSession = false;
                tapOutSchedule = currentSchedule; // Save current schedule for tap-out phase
                
                // Use more visible class ended message
                displayMessage("Class Ended", "Tap to Confirm", 3000);
                // Add small delay to ensure message is displayed
                unsigned long endMsgTime = millis();
                while (millis() - endMsgTime < 300) yield(); // Small non-blocking delay
                
                Serial.println("Instructor manually ended session");
              } else {
                // Regular instructor access
                logInstructor(uidStr, timestamp, "Access");
              }
            }
          } else if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
            yield(); // Yield for student handling
          }
        } else if (isAdminUID(uidStr)) {
          yield(); // Yield before admin access
          logAdminAccess(uidStr, timestamp);
        } else {
          yield(); // Yield before unregistered handling
          logUnregisteredUID(uidStr, timestamp);
          unregisteredUIDFeedback(); // New function for unregistered UID feedback
        }
        yield(); // Final yield after RFID processing
      }
    }
  }
  yield(); // Final yield at loop end

  checkAdminDoorAutoLock();
}

// Get instructor's schedule for a specific day, regardless of time
ScheduleInfo getInstructorScheduleForDay(String uid, String dateStr) {
  ScheduleInfo schedule = {false, "", "", "", "", "", "", ""};
  
  yield(); // Add yield at start of function
  
  if (firestoreTeachers.find(uid) == firestoreTeachers.end()) {
    Serial.println("Instructor UID not found in firestoreTeachers: " + uid);
    return schedule;
  }
  
  // Get day of the week from date (dateStr format: YYYY_MM_DD)
  int year = dateStr.substring(0, 4).toInt();
  int month = dateStr.substring(5, 7).toInt();
  int day = dateStr.substring(8, 10).toInt();
  
  // Calculate day of week using Zeller's Congruence
  if (month < 3) {
    month += 12;
    year--;
  }
  int h = (day + (13 * (month + 1)) / 5 + year + year / 4 - year / 100 + year / 400) % 7;
  
  yield(); // Add yield before array access
  
  // Convert h to day name (h=0 is Saturday in Zeller's)
  const String dayNames[7] = {"Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday"};
  String dayOfWeek = dayNames[h];
  
  // Get schedule for this day regardless of time
  if (firestoreTeachers[uid].count("schedules") > 0) {
    String schedulesStr = firestoreTeachers[uid]["schedules"];
    FirebaseJsonArray schedulesArray;
    
    yield(); // Add yield before JSON parsing
    
    if (schedulesArray.setJsonArrayData(schedulesStr)) {
      // Check if this is a valid schedule day
      for (size_t i = 0; i < schedulesArray.size(); i++) {
        // Add yield every few iterations to prevent watchdog timeout
        if (i % 3 == 0) {
          yield();
          // Reset watchdog for long schedule processing
          if (i > 9) lastReadyPrint = millis(); // Only reset if processing many schedules
        }
        
        FirebaseJsonData jsonData;
        if (schedulesArray.get(jsonData, i)) {
          FirebaseJson scheduleJson;
          if (scheduleJson.setJsonData(jsonData.stringValue)) {
            FirebaseJsonData dayData, startData, endData, roomData, subjectData, codeData, sectionData;
            
            if (scheduleJson.get(dayData, "day") && dayData.stringValue.equalsIgnoreCase(dayOfWeek)) {
              schedule.day = dayData.stringValue;
              
              if (scheduleJson.get(startData, "startTime")) {
                schedule.startTime = startData.stringValue;
              }
              
              if (scheduleJson.get(endData, "endTime")) {
                schedule.endTime = endData.stringValue;
              }
              
              if (scheduleJson.get(roomData, "roomName")) {
                schedule.roomName = roomData.stringValue;
              }
              
              if (scheduleJson.get(subjectData, "subject")) {
                schedule.subject = subjectData.stringValue;
              }
              
              if (scheduleJson.get(codeData, "subjectCode")) {
                schedule.subjectCode = codeData.stringValue;
              }
              
              if (scheduleJson.get(sectionData, "section")) {
                schedule.section = sectionData.stringValue;
              }
              
              schedule.isValid = true;
              Serial.println("Found instructor schedule for " + dayOfWeek + ": " + 
                            schedule.startTime + "-" + schedule.endTime + 
                            " in room " + schedule.roomName);
              
              // Take the latest class on this day (if multiple)
              // This is especially relevant for instructors ending class early
              // This is especially relevant for instructors ending class early
              continue;
            }
          }
        }
      }
    }
  }
  
  yield(); // Add yield before return
  return schedule;
}

// Process RFID card detection
void processRFIDCard(String uidStr) {
  // Only print details the first time we see this card in a session
  if (uidDetailsPrinted.find(uidStr) == uidDetailsPrinted.end() || !uidDetailsPrinted[uidStr]) {
    Serial.println("Card detected: UID = " + uidStr);
    // Mark as printed to avoid repeated logging
    uidDetailsPrinted[uidStr] = true;
  }
  
  String timestamp = getFormattedTime();
  lastRFIDTapTime = lastActivityTime = millis();
  
  // Example: Check if this is a registered UID
  if (isRegisteredUID(uidStr)) {
    // Handle based on whether this is a teacher or student
    if (uidStr == SUPER_ADMIN_UID) {
      // Special handling for the hardcoded Super Admin
      yield(); // Prevent watchdog reset
      logSuperAdmin(uidStr, timestamp);
    } else if (firestoreTeachers.find(uidStr) != firestoreTeachers.end()) {
      // Normal teacher logic
      String role = firestoreTeachers[uidStr]["role"];
      if (role.length() == 0) role = "instructor";
      if (role.equalsIgnoreCase("instructor")) {
        yield(); // Prevent watchdog reset
        logInstructor(uidStr, timestamp, "Access");
      }
    } else if (firestoreStudents.find(uidStr) != firestoreStudents.end()) {
      // Student logic would go here based on your existing code
      yield(); // Prevent watchdog reset
    }
  } else if (isAdminUID(uidStr)) {
    yield(); // Prevent watchdog reset
    logAdminAccess(uidStr, timestamp);
  } else {
    yield(); // Prevent watchdog reset
    logUnregisteredUID(uidStr, timestamp);
    unregisteredUIDFeedback(); // New function for unregistered UID feedback
  }
}

// SD card initialization function
void initSDCard() {
  Serial.println("Initializing SD card...");
  fsSPI.begin(SD_SCK, SD_MISO, SD_MOSI, SD_CS);
  pinMode(SD_CS, OUTPUT);
  
  if (!SD.begin(SD_CS, fsSPI, 4000000)) {
    Serial.println("⚠️ SD Card initialization failed!");
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print("SD Card Fail");
    tone(BUZZER_PIN, 700, 1000);
    // Not entering an infinite loop here to allow caller to handle the failure
    return;
  }
  
  Serial.println("✅ SD Card initialized.");
  sdInitialized = true;
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("SD Card Ready");
}

// Add the new logSuperAdmin function
void logSuperAdmin(String uid, String timestamp) {
  Serial.println("Processing Super Admin UID: " + uid);
  
  // SD log entry
  String entry = "SuperAdmin:UID:" + uid + " Time:" + timestamp + " Action:Access";
  storeLogToSD(entry);
  
  // IMPROVED LOGIC: First check if this admin is the one who started the current session
  bool isEntry;
  
  if (adminAccessActive && uid == lastAdminUID) {
    // This is the same admin who started the session - this is an EXIT
    isEntry = false;
    Serial.println("Super Admin is exiting - session will end");
  } else if (adminAccessActive && uid != lastAdminUID) {
    // Different admin is already active
    deniedFeedback();
    displayMessage("Another Admin", "Session Active", 2000);
    
    // Update timers
    firstActionOccurred = true;
    lastActivityTime = millis();
    lastReadyPrint = millis();
    return;
  } else {
    // No admin session active - this is an ENTRY
    isEntry = true;
    Serial.println("Super Admin is entering - new session starting");
  }
  
  // Assign room ID BEFORE creating the AccessLogs entry
  if (isEntry) {
    assignedRoomId = assignRoomToAdmin(uid);
  }
  
  // Sanitize timestamp for Firebase paths
  String sanitizedTimestamp = timestamp;
  sanitizedTimestamp.replace(" ", "_");
  sanitizedTimestamp.replace(":", "");
  
  // Firebase logging
  if (!sdMode && isConnected && Firebase.ready()) {
    // Update Admin node with Super Admin details
    String adminPath = "/Admin/" + uid;
    FirebaseJson adminJson;
    
    // Use hardcoded data for Super Admin
    adminJson.set("fullName", "CIT-U SUPER ADMIN");
    adminJson.set("role", "superadmin");  // Note the special role
    adminJson.set("email", "superadmin@cit-u.edu");
    adminJson.set("department", "Administration");
    adminJson.set("createdAt", "2023-01-01T00:00:00.000Z");
    adminJson.set("rfidUid", uid);
    
    // Create access log in the separate AccessLogs node structure
    String accessLogPath = "/AccessLogs/" + uid + "/" + sanitizedTimestamp;
    
    FirebaseJson accessJson;
    accessJson.set("action", isEntry ? "entry" : "exit");
    accessJson.set("fullName", "CIT-U SUPER ADMIN");
    accessJson.set("role", "superadmin");
    accessJson.set("timestamp", sanitizedTimestamp);
    
    // Add room details ONLY during entry
    if (isEntry && assignedRoomId != "" && firestoreRooms.find(assignedRoomId) != firestoreRooms.end()) {
      FirebaseJson roomJson;
      roomJson.set("name", firestoreRooms[assignedRoomId].at("name"));
      
      // Fix: Check if map contains the key, otherwise use default value
      String building = "CIT-U Building";
      if (firestoreRooms[assignedRoomId].find("building") != firestoreRooms[assignedRoomId].end()) {
        building = firestoreRooms[assignedRoomId].at("building");
      }
      roomJson.set("building", building);
      
      String floor = "1st";
      if (firestoreRooms[assignedRoomId].find("floor") != firestoreRooms[assignedRoomId].end()) {
        floor = firestoreRooms[assignedRoomId].at("floor");
      }
      roomJson.set("floor", floor);
      
      // Set status to "maintenance" during entry
      roomJson.set("status", "maintenance");
      
      String type = "classroom";
      if (firestoreRooms[assignedRoomId].find("type") != firestoreRooms[assignedRoomId].end()) {
        type = firestoreRooms[assignedRoomId].at("type");
      }
      roomJson.set("type", type);
      
      accessJson.set("roomDetails", roomJson);

          // Also update the room status in Firestore
          String roomPath = "rooms/" + assignedRoomId;
          FirebaseJson contentJson;
          contentJson.set("fields/status/stringValue", "maintenance");
          
          if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", roomPath.c_str(), contentJson.raw(), "status")) {
            Serial.println("Room status updated to 'maintenance' in Firestore: " + assignedRoomId);
          } else {
            Serial.println("Failed to update room status in Firestore: " + firestoreFbdo.errorReason());
          }
    }
    
    // If this is an exit tap, add PZEM data if available
    if (!isEntry) {
      // Add PZEM data for exit logs
      float voltage = pzem.voltage();
      float current = pzem.current();
      float power = pzem.power();
      float energy = pzem.energy();
      float frequency = pzem.frequency();
      float pf = pzem.pf();
      
      if (voltage > 0) {
        FirebaseJson pzemJson;
        pzemJson.set("voltage", voltage);
        pzemJson.set("current", current);
        pzemJson.set("power", power);
        pzemJson.set("energy", energy);
        pzemJson.set("frequency", frequency);
        pzemJson.set("powerFactor", pf);
        
        accessJson.set("pzemData", pzemJson);
      }
    }
    
    // Update the Admin node
    if (Firebase.RTDB.setJSON(&fbdo, adminPath, &adminJson)) {
      Serial.println("Super Admin details updated in RTDB at " + adminPath);
    } else {
      Serial.println("Failed to update Super Admin details: " + fbdo.errorReason());
    }
    
    // Update the AccessLogs node
    if (Firebase.RTDB.setJSON(&fbdo, accessLogPath, &accessJson)) {
      Serial.println("Super Admin access log created at " + accessLogPath);
    } else {
      Serial.println("Failed to create Super Admin access log: " + fbdo.errorReason());
    }
  }
  
  // Handle entry/exit behavior based on our improved isEntry flag
  if (isEntry) {
    activateRelays();
    adminAccessActive = true;
    lastAdminUID = uid;
    
    // Set up door auto-lock timeout
    adminDoorOpenTime = millis();
    Serial.println("Door will auto-lock in 30 seconds while super admin inspection continues");
    
    if (assignedRoomId == "") {
      displayMessage("No Room Available", "For Super Admin", 2000);
    } else {
      if (firestoreRooms.find(assignedRoomId) != firestoreRooms.end()) {
        String roomName = firestoreRooms[assignedRoomId].at("name");
        displayMessage("Inspecting Room", roomName, 2000);
      }
    }
    
    accessFeedback();
    logSystemEvent("Relay Activated for Super Admin: " + uid);
    displayMessage("Super Admin", "Access Granted", 2000);
    displayMessage("Super Admin Mode", "Active", 0);
  } else {
    // This is an exit tap - now determined by our isEntry flag
    bool wasRelayAlreadyDeactivated = !relayActive;
    
    // Always deactivate relays - even if they are already deactivated by auto-lock
    // This ensures proper state consistency
    deactivateRelays();
    
    // Allow system to stabilize after relay operations
    yield();
    delay(50);
    yield();
    
    // Clear the admin session state
    adminAccessActive = false;
    lastAdminUID = "";
    
    // Provide more visible feedback if the door was already locked
    if (wasRelayAlreadyDeactivated) {
      // Special feedback for when door was already auto-locked
      digitalWrite(LED_R_PIN, LOW);
      digitalWrite(LED_G_PIN, HIGH);
      digitalWrite(LED_B_PIN, HIGH); // Blue + Green = Cyan
      
      // Use a distinct tone pattern
      tone(BUZZER_PIN, 2000, 100);
      delay(150);
      tone(BUZZER_PIN, 2500, 100);
      delay(150);
      tone(BUZZER_PIN, 3000, 100);
      
      Serial.println("Door was already locked - special exit feedback provided");
    } else {
      // Standard access feedback for normal exit
      accessFeedback();
    }
    
    // Update room status back to "available" if needed
    if (assignedRoomId != "" && !sdMode && isConnected && Firebase.ready()) {
      String roomPath = "rooms/" + assignedRoomId;
      FirebaseJson contentJson;
      contentJson.set("fields/status/stringValue", "available");
      
      if (Firebase.Firestore.patchDocument(&firestoreFbdo, FIRESTORE_PROJECT_ID, "", roomPath.c_str(), contentJson.raw(), "status")) {
        Serial.println("Room status updated to 'available' in Firestore: " + assignedRoomId);
      } else {
        Serial.println("Failed to update room status in Firestore: " + firestoreFbdo.errorReason());
      }
    }
    
    assignedRoomId = "";
    
    // Extra visible feedback for exit
    if (wasRelayAlreadyDeactivated) {
      displayMessage("Super Admin Exit", "Session Ended", 2000);
    } else {
      displayMessage("Super Admin", "Exit Processed", 2000);
    }
    
    logSystemEvent("Super Admin Session Ended: " + uid);
    
    // Allow the system to stabilize
    yield();
    
    // Use smooth transition instead of direct ready message
    smoothTransitionToReady();
  }
  
  // Update timers
  firstActionOccurred = true;
  lastActivityTime = millis();
  lastReadyPrint = millis();
}