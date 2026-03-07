from datetime import datetime
import time
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
import queue

# --- Configuration for Multiple PLCs ---
PLC_CONFIGS = [
    {"ip": "192.168.1.101", "port": 5001, "station": "STATION 04", "enabled": True},
    {"ip": "192.168.1.103", "port": 5001, "station": "STATION 07", "enabled": True},
    {"ip": "192.168.1.104", "port": 5001, "station": "STATION 06", "enabled": True},
    {"ip": "192.168.1.105", "port": 5001, "station": "STATION 05", "enabled": True},
    {"ip": "192.168.1.106", "port": 5001, "station": "STATION 14", "enabled": True},
     {"ip": "192.168.1.107", "port": 5001, "station": "OFFLINE STATION", "enabled": True}, 
    
]

# --- Global Settings ---
POLL_INTERVAL = 3
HEARTBEAT_INT = 5
BYTE_ORDER = 'little'
MAX_RETRIES = 3
RETRY_DELAY = 5

# SQL Configuration
SQL_SERVER = "localhost"
SQL_DATABASE = "PLC_Monitoring"
SQL_TABLE = "Production_Log"
ERROR_TABLE = "System_Error_Log"
USE_WINDOWS_AUTH = True

# --- Setup & Libraries ---
try:
    from pymcprotocol import Type3E
    import pyodbc
except ImportError as e:
    print(f"❌ Missing required library: {e}")
    sys.exit(1)

# --- SQL Connection Pool ---
class SQLConnectionPool:
    def __init__(self):
        self.conn = None
        self.lock = threading.Lock()
    
    def get_connection(self):
        with self.lock:
            if self.conn:
                try:
                    self.conn.cursor().execute("SELECT 1")
                    return self.conn
                except:
                    self.conn = None
            
            try:
                auth = "Trusted_Connection=yes;" if USE_WINDOWS_AUTH else ""
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};{auth}"
                self.conn = pyodbc.connect(conn_str, timeout=5, autocommit=False)
                
                # Create error table if not exists
                cursor = self.conn.cursor()
                cursor.execute(f"""
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{ERROR_TABLE}')
                    CREATE TABLE {ERROR_TABLE} (
                        ID INT IDENTITY(1,1) PRIMARY KEY,
                        Timestamp DATETIME DEFAULT GETDATE(),
                        StationName NVARCHAR(50),
                        ErrorMessage NVARCHAR(MAX),
                        ErrorSource NVARCHAR(50),
                        PLC_IP NVARCHAR(15)
                    )
                """)
                self.conn.commit()
                return self.conn
            except Exception as e:
                print(f"[{get_timestamp()}] ⚠️ SQL Connection failed: {e}")
                return None
    
    def close(self):
        with self.lock:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
                self.conn = None

sql_pool = SQLConnectionPool()

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def decode_plc_ascii(data):
    result = []
    for value in data:
        lo = value & 0xFF
        hi = (value >> 8) & 0xFF
        chars = [lo, hi] if BYTE_ORDER == 'little' else [hi, lo]
        for c in chars:
            if c == 0:
                return ''.join(result)
            if 32 <= c <= 126:
                result.append(chr(c))
    return ''.join(result)

def log_error(plc_ip, station_name, error_msg, source):
    timestamp = get_timestamp()
    print(f"[{timestamp}] ⚠️ [{station_name}] {source}: {error_msg}")
    
    conn = sql_pool.get_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                INSERT INTO {ERROR_TABLE} (StationName, ErrorMessage, ErrorSource, PLC_IP) 
                VALUES (?, ?, ?, ?)
            """, station_name, str(error_msg), source, plc_ip)
            conn.commit()
        except:
            pass

def insert_production_data(plc_ip, station_name, data):
    if data["cycle"] == 0:
        return False
    
    conn = sql_pool.get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {SQL_TABLE} 
            (StationNumber, ModelName, ModelNumber, SetTorque, CycleTime, Status_OK_NG, PLC_IP) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, 
            station_name,
            data["model_name"][:50],  # Truncate if too long
            data["model_num"],
            data["torque"],
            data["cycle"],
            data["status"],
            plc_ip
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"[{get_timestamp()}] ❌ SQL Error [{station_name}]: {e}")
        return False

def monitor_plc(plc_config):
    plc_ip = plc_config["ip"]
    plc_port = plc_config["port"]
    station_name = plc_config["station"]
    
    print(f"[{get_timestamp()}] 🔌 Starting: {station_name} ({plc_ip})")
    
    plc = Type3E()
    connected = False
    last_m1000 = None
         # Track the previous value of D2310
    has_seen_zero = True # Initialize as True so the very first non-zero value logs
    last_heartbeat = 0
    retry_count = 0
    last_captured = {"model_name": "N/A", "model_num": 0, "torque": 0, "cycle": 0, "status": "N/A"}
    
    while plc_config.get("enabled", True):
        try:
            current_time = time.time()
            
            # Connect if needed
            if not connected:
                try:
                    plc.connect(plc_ip, plc_port)
                    connected = True
                    retry_count = 0
                    print(f"[{get_timestamp()}] ✅ Connected: {station_name}")
                except Exception as e:
                    retry_count += 1
                    if retry_count == 1:  # Only log first failure
                        log_error(plc_ip, station_name, e, "CONNECT")
                    time.sleep(RETRY_DELAY)
                    continue
            
            # Read PLC data
            try:
                m1000 = plc.batchread_bitunits("M77", 1)[0]
                current_d2310 = plc.batchread_wordunits("D2310", 1)[0]
            except Exception as e:
                connected = False
                log_error(plc_ip, station_name, e, "READ")
                continue
            
            # Trigger on rising edge
            if current_d2310 == 0:
                has_seen_zero = True
            elif has_seen_zero and current_d2310 > 0 and m1000==1 :
                try:
                    # Batch read all data
                    d1021_raw = plc.batchread_wordunits("D1021", 10)
                    d5000 = plc.batchread_wordunits("D5000", 1)[0]
                    d3041 = plc.batchread_wordunits("D3041", 1)[0]
                    d5075 = plc.batchread_wordunits("D5075", 1)[0]
                    
                    last_captured = {
                        "model_name": decode_plc_ascii(d1021_raw),
                        "model_num": d5075,
                        "torque": d5000,
                        "cycle": current_d2310,
                        "status": "OK" if d3041 == 19279 else "NG"
                    }
                    
                    if insert_production_data(plc_ip, station_name, last_captured):
                        print(f"[{get_timestamp()}] 💾 {station_name}: {last_captured['model_name']} (Cycle: {current_d2310})")
                    # Reset the toggle so we don't log the same value twice
                    has_seen_zero = False
                    
                except Exception as e:
                    log_error(plc_ip, station_name, e, "DATA")
            
            # Heartbeat
            if current_time - last_heartbeat >= HEARTBEAT_INT:
                status = "ON" if m1000 else "OFF"
                print(f"[{get_timestamp()}] ❤️ {station_name}: M1000={status} | Cycle={current_d2310}")
                last_heartbeat = current_time
            
            #last_m1000 = m1000
            
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            log_error(plc_ip, station_name, e, "LOOP")
            connected = False
            time.sleep(RETRY_DELAY)
    
    print(f"[{get_timestamp()}] 🛑 Stopped: {station_name}")

def main():
    print(f"\n{'='*60}")
    print(f"  MULTI-PLC MONITORING SYSTEM")
    print(f"  Started: {get_timestamp()}")
    print(f"  Active PLCs: {len([p for p in PLC_CONFIGS if p.get('enabled', True)])}")
    print(f"{'='*60}\n")
    
    # Test SQL connection
    if sql_pool.get_connection():
        print(f"[{get_timestamp()}] ✅ SQL Server connected")
    else:
        print(f"[{get_timestamp()}] ⚠️ SQL Server connection failed - will retry")
    
    # Start threads
    threads = []
    for config in PLC_CONFIGS:
        if config.get("enabled", True):
            t = threading.Thread(target=monitor_plc, args=(config,), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(0.2)  # Stagger connections
    
    print(f"\n[{get_timestamp()}] ✨ All monitors running. Press Ctrl+C to stop.\n")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
            # Optional: Print summary every minute
            if int(time.time()) % 60 == 0:
                active = sum(1 for t in threads if t.is_alive())
                print(f"[{get_timestamp()}] 📊 Active threads: {active}/{len(threads)}")
    except KeyboardInterrupt:
        print(f"\n[{get_timestamp()}] 🛑 Shutting down...")
        sql_pool.close()
        print(f"[{get_timestamp()}] 👋 Goodbye!")

if __name__ == "__main__":
    main()