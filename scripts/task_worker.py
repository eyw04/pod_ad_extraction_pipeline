import os
import sqlite3
import requests
import hashlib
import shutil
from datetime import datetime, timedelta
import fcntl

# --- CONFIGURATION ---
DB_PATH = "/shared/6/projects/podcast-ads/pipeline.db"
STORAGE_BASE = "/shared/6/projects/podcast-ads/"
PROJECT_MAX_GB = 100.0  
INTERVAL_HOURS = 10  
BATCH_SIZE = 100

def is_already_running():
    f = open('/tmp/podcast_worker.lock', 'w')
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except IOError:
        return None

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def get_file_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except:
        return None

def get_project_usage_gb(path):
    total_bytes = 0
    if not os.path.exists(path): return 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_bytes += os.path.getsize(fp)
    return total_bytes / (1024**3)

def check_system_free_gb(path):
    total, used, free = shutil.disk_usage(path)
    return free / (1024**3)

def get_scheduler_state():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("CREATE TABLE IF NOT EXISTS scheduler_config (key TEXT PRIMARY KEY, value TEXT)")
    cursor.execute("SELECT value FROM scheduler_config WHERE key = 'start_time'")
    row = cursor.fetchone()
    
    if row is None:
        start_time = datetime.now()
        cursor.execute("INSERT INTO scheduler_config (key, value) VALUES ('start_time', ?)", 
                       (start_time.strftime("%Y-%m-%d %H:%M:%S"),))
        conn.commit()
    else:
        start_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")

    cursor.execute("SELECT MAX(batch_id) FROM batches")
    res = cursor.fetchone()
    max_batch = res[0] if res and res[0] else 0
    # Decide the mode
    duration = datetime.now() - start_time
    total_hours = int(duration.total_seconds() // 3600)
    # decide the batch to run
    mode = 'v1' if (total_hours % 24) < 12 else 'v2'
    days_passed = total_hours // 24
    start_b = (days_passed * BATCH_SIZE) % (max_batch)
    end_b = min(start_b + BATCH_SIZE, max_batch)
    
    conn.close()
    return mode, (start_b, end_b)

def run_worker(mode, batch_range):
    min_b, max_b = batch_range
    conn = get_db()
    cursor = conn.cursor()
    
    seen_batches = set()
    print(f"[{datetime.now()}] Mode: {mode.upper()} | Range: Batch {min_b}-{max_b}")

    sql = """
        SELECT t.task_id, t.episode_id, e.url, t.current_version, t.batch_id
        FROM download_tasks t
        JOIN episodes e ON t.episode_id = e.episode_id
        WHERE t.status = ? AND t.batch_id BETWEEN ? AND ? 
    """
    
    if mode == 'v1':
        cursor.execute(sql + " LIMIT 400", (0, min_b, max_b))
    else:
        cursor.execute(sql + " AND t.scheduled_time <= CURRENT_TIMESTAMP LIMIT 400", (2, min_b, max_b))
    
    tasks = cursor.fetchall()

    for task_id, ep_id, url, current_ver, b_id in tasks:
        if b_id not in seen_batches:
            seen_batches.add(b_id)
            if len(seen_batches) % 20 == 0:
                print(f"Progress: {len(seen_batches)} batches scanned...")

        new_version = current_ver + 1
        
        # Lock status to prevent concurrent duplicates
        cursor.execute("UPDATE download_tasks SET status = 1 WHERE task_id = ?", (task_id,))
        conn.commit()

        if check_system_free_gb(STORAGE_BASE) < 5.0:
            print("Disk critical. Stop.")
            cursor.execute("UPDATE download_tasks SET status = ? WHERE task_id = ?", (current_ver * 2, task_id))
            conn.commit()
            break

        my_usage = get_project_usage_gb(STORAGE_BASE)
        if new_version == 1 and my_usage > PROJECT_MAX_GB:
            cursor.execute("UPDATE download_tasks SET status = 0 WHERE task_id = ?", (task_id,))
            conn.commit()
            continue

        safe_filename = hashlib.md5(ep_id.encode()).hexdigest()
        file_ext = url.split('.')[-1].split('?')[0] or "mp3"
        RANGE_SIZE = 100 
        range_start = (b_id // RANGE_SIZE) * RANGE_SIZE
        range_end = range_start + RANGE_SIZE
        range_folder = f"range_{range_start:05d}_{range_end:05d}"

        target_dir = os.path.join(STORAGE_BASE, f"v{new_version}", range_folder)
        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, f"{safe_filename}.{file_ext}")
        
        temp_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        headers = {'Range': f'bytes={temp_size}-'} if temp_size > 0 else {}

        try:
            r = requests.get(url, timeout=60, stream=True, headers=headers)
            write_mode = 'ab' if r.status_code == 206 else 'wb'
            
            if r.status_code in [200, 206]:
                with open(file_path, write_mode) as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk: f.write(chunk)
                
                md5_val = get_file_md5(file_path)
                cursor.execute("""
                    INSERT INTO downloads (task_id, episode_id, md5_checksum, file_path, download_version, status_code) 
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (task_id, ep_id, md5_val, file_path, new_version, r.status_code))

                if new_version == 1:
                    next_run = (datetime.now() + timedelta(hours=INTERVAL_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
                    cursor.execute("""
                        UPDATE download_tasks 
                        SET status = 2, current_version = 1, scheduled_time = ? 
                        WHERE task_id = ?
                    """, (next_run, task_id))
                else:
                    cursor.execute("UPDATE download_tasks SET status = 3, current_version = 2 WHERE task_id = ?", (task_id,))
                conn.commit()
            else:
                cursor.execute("UPDATE download_tasks SET status = ? WHERE task_id = ?", (current_ver * 2, task_id))
                conn.commit()
        except Exception:
            cursor.execute("UPDATE download_tasks SET status = ? WHERE task_id = ?", (current_ver * 2, task_id))
            conn.commit()

    conn.close()

if __name__ == "__main__":
    lock_file = is_already_running()
    if not lock_file:
        print("Another instance is already running. Exiting.")
        sys.exit(0)
        
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=['v1', 'v2'], help="Force mode v1 or v2")
    parser.add_argument("--start", type=int, help="Batch start ID")
    parser.add_argument("--end", type=int, help="Batch end ID")
    args = parser.parse_args()

    if args.mode and args.start is not None and args.end is not None:
        state = (args.mode, (args.start, args.end))
        print(f"[{datetime.now()}] Using command line arguments.")
    else:
        state = get_scheduler_state()
        print(f"[{datetime.now()}] Using internal scheduler logic.")

    if state:
        run_worker(*state)

# def execute_downloads():
#     conn = sqlite3.connect(DB_PATH)
#     cursor = conn.cursor()

#     cursor.execute("""
#         SELECT t.task_id, t.episode_id, e.url, t.current_version, t.batch_id
#         FROM download_tasks t
#         JOIN episodes e ON t.episode_id = e.episode_id
#         WHERE t.status = 0
#         AND t.scheduled_time <= CURRENT_TIMESTAMP
#         AND t.current_version < 2
#         ORDER BY t.batch_id ASC, t.current_version DESC
#     """)
#     tasks = cursor.fetchall()

#     for task_id, ep_id, url, old_version, batch_id in tasks:
#         if batch_id > 3:
#             return
#         new_version = old_version + 1
        
#         # 1. Check system physical limit (Safety first)
#         sys_free = check_system_free_gb(STORAGE_BASE)
#         if sys_free < 5.0:  # If less than 5GB left on /shared/3, stop.
#             print(f"System storage critical ({sys_free:.2f}GB left). Aborting.")
#             break

#         # 2. Check your 100GB personal quota
#         my_usage = get_project_usage_gb(STORAGE_BASE)
#         if new_version == 1 and my_usage > PROJECT_MAX_GB:
#             print(f"Personal quota reached ({my_usage:.2f}GB). Skipping V1 task.")
#             continue
        
#         target_dir = os.path.join(STORAGE_BASE, f"v{new_version}")
#         os.makedirs(target_dir, exist_ok=True)
#         safe_filename = hashlib.md5(ep_id.encode()).hexdigest()
#         file_ext = url.split('.')[-1].split('?')[0] or "mp3"
#         file_path = os.path.join(target_dir, f"{safe_filename}.{file_ext}")

#         try:
#             print(f"Downloading Batch {batch_id} - Task {task_id} (V{new_version})")
#             r = requests.get(url, timeout=60, stream=True)
#             if r.status_code == 200:
#                 with open(file_path, 'wb') as f:
#                     for chunk in r.iter_content(chunk_size=8192):
#                         f.write(chunk)
                
#                 md5 = get_file_md5(file_path)
                
#                 cursor.execute("""
#                     INSERT INTO downloads (task_id, episode_id, md5_checksum, file_path, download_version, status_code)
#                     VALUES (?, ?, ?, ?, ?, ?)
#                 """, (task_id, ep_id, md5, file_path, new_version, 200))

#                 # if new_version >= 2:
#                 #     cursor.execute("""
#                 #         SELECT file_path FROM downloads 
#                 #         WHERE task_id = ? AND download_version < ?
#                 #     """, (task_id, new_version))
#                 #     old_files = cursor.fetchall()
#                 #     for (old_path,) in old_files:
#                 #         if os.path.exists(old_path) and old_path != file_path:
#                 #             os.remove(old_path)
#                 #             print(f"Deleted old version: {old_path}")

#                 cursor.execute("""
#                     UPDATE download_tasks 
#                     SET status = 2, 
#                         current_version = ?,
#                         scheduled_time = datetime(CURRENT_TIMESTAMP, '+24 hours')
#                     WHERE task_id = ?
#                 """, (new_version, task_id))
                
#                 conn.commit()
#             else:
#                 print(f"HTTP {r.status_code} for task {task_id}")
#         except Exception as e:
#             print(f"Error on task {task_id}: {str(e)}")

#     conn.close()

# def reset_scheduled_tasks():
#     conn = sqlite3.connect(DB_PATH)
#     cursor = conn.cursor()
#     cursor.execute("""
#         UPDATE download_tasks
#         SET status = 0
#         WHERE status = 2 
#         AND scheduled_time <= CURRENT_TIMESTAMP
#     """)
#     conn.commit()
#     conn.close()
