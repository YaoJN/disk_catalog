import sqlite3
import os
import shutil
from datetime import datetime

def get_disk_info(file_path):
    with open(file_path, encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines()]

    disk_drive = lines[1]
    if not disk_drive.endswith("\\"):
        disk_drive += "\\"

    return {
        "disk_no": int(lines[0]),
        "disk_drive": disk_drive,
        "model": lines[2],
        "serial_number": lines[3],
        "speed": lines[4]
    }

def get_disk_usage(drive):
    usage = shutil.disk_usage(drive)
    total_gb = round(usage.total / (1024 ** 3))
    used_gb = round(usage.used / (1024 ** 3))
    free_gb = round(usage.free / (1024 ** 3))
    percent = round(used_gb / total_gb * 100)
    return total_gb, used_gb, free_gb, percent

def judge_health(percent):
    if percent >= 95:
        return "Critical"
    elif percent >= 90:
        return "Warning"
    else:
        return "Healthy"

def init_resource_data_table(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS resource_data (
        disk_no INTEGER,
        sub_no INTEGER,
        file_path TEXT,
        file_name TEXT,
        file_size INTEGER,
        file_created_time TEXT,
        file_updated_time TEXT,
        create_date TEXT,
        is_deleted INTEGER DEFAULT 0,
        PRIMARY KEY (disk_no, sub_no)
    )
    """)

def confirm_and_delete_files(conn, cur, disk_no):
    # 查询所有 is_deleted=1 的文件
    cur.execute("SELECT file_path, file_name FROM resource_data WHERE disk_no=? AND is_deleted=1", (disk_no,))
    to_delete_files = cur.fetchall()
    if not to_delete_files:
        print("无需删除的已标记文件。")
        return

    print("以下文件标记为删除，是否执行物理删除？(y/n)")
    for idx, (fp, fn) in enumerate(to_delete_files, 1):
        print(f"{idx}. {os.path.join(fp, fn)}")

    answer = input("请输入 y 确认删除，其他键取消：").strip().lower()
    if answer != 'y':
        print("取消删除操作。")
        return

    # 删除文件及数据库记录
    for fp, fn in to_delete_files:
        full_path = os.path.join(fp, fn)
        try:
            if os.path.isfile(full_path):
                os.remove(full_path)
                print(f"已删除文件：{full_path}")
            else:
                print(f"文件不存在，跳过：{full_path}")
        except Exception as e:
            print(f"删除文件失败：{full_path}，错误：{e}")

    # 删除数据库中对应记录
    cur.execute("DELETE FROM resource_data WHERE disk_no=? AND is_deleted=1", (disk_no,))
    conn.commit()
    print("已删除对应数据库记录。")

def update_resource_data(info, conn, cur):
    disk_no = info['disk_no']
    drive = info['disk_drive']
    now = datetime.now().isoformat()
    target_folder = os.path.join(drive, 'coldStorage')
    skip_exts = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.txt', '.nfo', '.srt', '.webm', '.arw', '.db', '.thm', '.jpg_56x42', '.jpg_170x128', '.jpg_320x240']

    # 先执行删除确认操作
    confirm_and_delete_files(conn, cur, disk_no)

    # 删除该硬盘历史数据
    cur.execute("DELETE FROM resource_data WHERE disk_no = ?", (disk_no,))

    sub_no = 1

    for root, dirs, files in os.walk(target_folder):
        filtered_files = [f for f in files if os.path.splitext(f)[1].lower() not in skip_exts]

        relative_path = os.path.relpath(root, target_folder)
        parts = relative_path.split(os.sep) if relative_path != '.' else []
        category1 = parts[0] if len(parts) >= 1 else ''
        category2 = parts[1] if len(parts) >= 2 else ''
        category3 = parts[2] if len(parts) >= 3 else ''

        if len(dirs) > 0:
            # 不是最后一层文件夹，不写文件夹记录，只写有效文件
            for file in filtered_files:
                full_path = os.path.join(root, file)
                try:
                    stat = os.stat(full_path)
                    file_size_mb = round(stat.st_size / (1024 * 1024))
                    created_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
                    updated_time = datetime.fromtimestamp(stat.st_mtime).isoformat()

                    cur.execute("""
                        INSERT INTO resource_data 
                        (disk_no, sub_no, category1, category2, category3, file_path, file_name, file_size, file_created_time, file_updated_time, create_date, is_deleted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (
                        disk_no, sub_no, category1, category2, category3, root, file, file_size_mb, created_time, updated_time, now
                    ))
                    sub_no += 1
                except Exception as e:
                    print(f"跳过异常文件：{full_path}, 错误: {e}")
            continue

        if len(filtered_files) == 0:
            cur.execute("""
                INSERT INTO resource_data 
                (disk_no, sub_no, category1, category2, category3, file_path, file_name, file_size, file_created_time, file_updated_time, create_date, is_deleted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """, (
                disk_no, sub_no, category1, category2, category3, root, '[FOLDER]', 0, now, now, now
            ))
            sub_no += 1

        for file in filtered_files:
            full_path = os.path.join(root, file)
            try:
                stat = os.stat(full_path)
                file_size_mb = round(stat.st_size / (1024 * 1024))
                created_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
                updated_time = datetime.fromtimestamp(stat.st_mtime).isoformat()

                cur.execute("""
                    INSERT INTO resource_data 
                    (disk_no, sub_no, category1, category2, category3, file_path, file_name, file_size, file_created_time, file_updated_time, create_date, is_deleted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, (
                    disk_no, sub_no, category1, category2, category3, root, file, file_size_mb, created_time, updated_time, now
                ))
                sub_no += 1
            except Exception as e:
                print(f"跳过异常文件：{full_path}, 错误: {e}")

    print(f"Disk {disk_no}: {sub_no - 1} 条文件记录已写入。")

def update_disk_info(disk_info_path, db_path):
    info = get_disk_info(disk_info_path)
    total, used, free, percent = get_disk_usage(info['disk_drive'])
    health = judge_health(percent)
    now = datetime.now().isoformat()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 确保表存在
    cur.execute("""
        CREATE TABLE IF NOT EXISTS resource_data (
            disk_no INTEGER,
            sub_no INTEGER,
            category1 TEXT,
            category2 TEXT,
            category3 TEXT,
            file_path TEXT,
            file_name TEXT,
            file_size INTEGER,
            file_created_time TEXT,
            file_updated_time TEXT,
            create_date TEXT,
            is_deleted INTEGER DEFAULT 0,
            PRIMARY KEY (disk_no, sub_no)
        )
    """)
    init_resource_data_table(cur)

    cur.execute("SELECT * FROM disk_info WHERE disk_no = ?", (info['disk_no'],))
    exists = cur.fetchone()

    if exists:
        cur.execute("""
            UPDATE disk_info SET 
                total = ?, used = ?, free = ?, percent = ?, health_status = ?, update_date = ?
            WHERE disk_no = ?
        """, (total, used, free, percent, health, now, info['disk_no']))
    else:
        cur.execute("""
            INSERT INTO disk_info 
            (disk_no, disk_drive, model, serial_number, speed, total, used, free, percent, health_status, create_date, update_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            info['disk_no'], info['disk_drive'], info['model'], info['serial_number'], info['speed'],
            total, used, free, percent, health, now, now
        ))

    conn.commit()

    print(f"Disk {info['disk_no']} updated: {health}, {percent:.2f}% used")

    # 更新 resource_data
    update_resource_data(info, conn, cur)

    conn.commit()
    conn.close()
    print("Resource data completed.")

def load_config_path(default_path):
    import sys
    config_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), "config.txt")
    if not os.path.exists(config_file):
        return default_path
    with open(config_file, encoding="utf-8") as f:
        for line in f:
            if line.startswith("db_path="):
                return line.strip().split("=", 1)[1]
    return default_path

if __name__ == '__main__':
    import sys

    current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    disk_info_path = os.path.join(current_dir, "DiskInformation.txt")

    if not os.path.exists(disk_info_path):
        print(f"❌ 找不到 DiskInformation.txt: {disk_info_path}")
        sys.exit(1)

    try:
        with open(disk_info_path, encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines()]
        if len(lines) < 6:
            raise ValueError("DiskInformation.txt 至少需要6行，第六行为数据库目录")
        db_dir = lines[5]
        if not os.path.exists(db_dir):
            print(f"⚠️ 数据库目录不存在，正在创建: {db_dir}")
            os.makedirs(db_dir, exist_ok=True)
        db_path = os.path.join(db_dir, "disk_info.db")
    except Exception as e:
        print(f"❌ 无法读取数据库路径: {e}")
        sys.exit(1)

    if not os.path.exists(db_path):
        print(f"⚠️ 数据库文件不存在，将创建新文件：{db_path}")
    else:
        print(f"✅ 使用数据库文件：{db_path}")

    update_disk_info(disk_info_path, db_path)
