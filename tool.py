import os
import shutil

source_dir = r'Z:\coldStorage\book\Novel'
target_dir = r'W:\coldStorage\book备份\Novel'

for root, dirs, files in os.walk(source_dir):
    for file in files:
        if 'epub' in file.lower():  # 文件名含epub（不区分大小写）
            src_file_path = os.path.join(root, file)

            # 生成目标路径
            relative_path = os.path.relpath(root, source_dir)
            dest_folder_path = os.path.join(target_dir, relative_path)
            dest_file_path = os.path.join(dest_folder_path, file)

            # 如果目标文件已存在，跳过
            if os.path.exists(dest_file_path):
                print(f"跳过: {dest_file_path} 已存在")
                continue

            # 确保目标文件夹存在
            os.makedirs(dest_folder_path, exist_ok=True)

            # 拷贝文件
            shutil.copy2(src_file_path, dest_file_path)
            print(f"已拷贝: {src_file_path} -> {dest_file_path}")

print("完成")
