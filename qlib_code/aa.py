import os
import re
import shutil
from datetime import datetime, timedelta

def reverse_shift_dates(source_folder, target_folder):
    """
    反向shift：后面的文件使用前面文件的日期
    例如：3号文件变成1号，5号文件变成3号
    """
    os.makedirs(target_folder, exist_ok=True)
    
    pattern = r'prediction_(\d{8})\.csv'
    files_info = []
    
    # 收集文件信息
    for filename in os.listdir(source_folder):
        match = re.match(pattern, filename)
        if match:
            date_str = match.group(1)
            try:
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                files_info.append({'filename': filename, 'date': date_obj, 'date_str': date_str})
            except ValueError:
                continue
    
    if len(files_info) < 2:
        print("文件数量不足")
        return
    
    # 按日期排序（从早到晚）
    files_info.sort(key=lambda x: x['date'])
    
    print("原文件顺序（按日期排序）:")
    for i, info in enumerate(files_info):
        print(f"{i+1}. {info['filename']} (日期: {info['date_str']})")
    
    # 反向shift：后面的文件取前面文件的日期
    export_plan = []
    for i in range(len(files_info)):
        if i == 0:
            # 第一个文件：日期保持不变（或者可以删除）
            continue
        else:
            # 当前文件取前一个文件的日期
            current_file = files_info[i]['filename']
            prev_file_date = files_info[i-1]['date']
            new_date_str = prev_file_date.strftime('%Y%m%d')
            new_filename = f'prediction_{new_date_str}.csv'
            
            export_plan.append({
                'source_file': current_file,
                'target_file': new_filename,
                'original_date': files_info[i]['date_str'],
                'new_date': new_date_str,
                'description': f"{files_info[i]['date_str']} -> {new_date_str}"
            })
    
    # 显示预览
    print("\n反向shift重命名预览:")
    print("-" * 60)
    print("注意：第一个文件将保持不变")
    for plan in export_plan:
        print(f"{plan['source_file']} -> {plan['target_file']}")
    print("-" * 60)
    
    response = input("确认执行？(y/n): ")
    if response.lower() != 'y':
        print("操作取消")
        return
    
    # 执行导出
    success_count = 0
    for plan in export_plan:
        try:
            source_path = os.path.join(source_folder, plan['source_file'])
            target_path = os.path.join(target_folder, plan['target_file'])
            
            if not os.path.exists(target_path):
                shutil.copy2(source_path, target_path)
                print(f"✓ {plan['source_file']} -> {plan['target_file']}")
                success_count += 1
            else:
                print(f"跳过: {plan['target_file']} 已存在")
                
        except Exception as e:
            print(f"✗ 错误: {plan['source_file']} - {e}")
    
    print(f"\n完成: 成功导出 {success_count}/{len(export_plan)} 个文件")

source_folder = r"E:\qlib_output"
target_folder = r"E:\qlib_outputtt"
reverse_shift_dates(source_folder, target_folder)