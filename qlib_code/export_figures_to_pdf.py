import os
from PIL import Image, ImageDraw, ImageFont
import glob
from datetime import datetime
import numpy as np
import pandas as pd

import qlib
from qlib.workflow import R
from qlib.config import REG_CN

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


qlib_initialized = False
recorder = None

def global_init(experiment_id, experiment_name, provider_uri):
    global qlib_initialized, recorder
    if not qlib_initialized:
        qlib.init(provider_uri=provider_uri, region=REG_CN)
        qlib_initialized = True
    recorder = R.get_recorder(experiment_id=experiment_id, experiment_name=experiment_name)

def create_info_page_with_first_image(experiment_id, experiment_name, provider_uri, image_path):
    
    # 页面尺寸 (A4)
    page_width = 2100  # 像素
    page_height = 2970  # 标准A4高度
    
    # 创建白色背景
    img = Image.new('RGB', (page_width, page_height), 'white')
    draw = ImageDraw.Draw(img)
    
    try:
        # 尝试使用中文字体
        # Windows系统常见的中文字体
        chinese_fonts = [
            "C:/Windows/Fonts/simhei.ttf",  # 黑体
            "C:/Windows/Fonts/simsun.ttc",  # 宋体
            "C:/Windows/Fonts/msyh.ttc",    # 微软雅黑
            "C:/Windows/Fonts/simkai.ttf",  # 楷体
        ]
        
        title_font = None
        content_font = None
        
        for font_path in chinese_fonts:
            if os.path.exists(font_path):
                try:
                    title_font = ImageFont.truetype(font_path, 80)
                    content_font = ImageFont.truetype(font_path, 40)
        
                    break
                except:
                    continue
        
        if title_font is None:
            raise Exception("未找到中文字体")
            
    except:
        # 如果中文字体不可用，使用默认字体
        log.warning("警告: 未找到中文字体，使用默认字体（可能无法正确显示中文）")
        title_font = ImageFont.load_default()
        content_font = ImageFont.load_default()
    
    # 标题
    global recorder
    global_init(experiment_id, experiment_name, provider_uri)

    

    config = recorder.load_object("config")

    model_type = config['task']['model']['class']
    title = f"{model_type}报告"
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (page_width - title_width) // 2
    draw.text((title_x, 200), title, fill='black', font=title_font)

    
    train_start = config['task']['dataset']['kwargs']['segments']['train'][0]
    train_end = config['task']['dataset']['kwargs']['segments']['train'][1]
    
    valid_start = config['task']['dataset']['kwargs']['segments']['valid'][0]
    valid_end = config['task']['dataset']['kwargs']['segments']['valid'][1]

    test_start = config['task']['dataset']['kwargs']['segments']['test'][0]
    test_end = config['task']['dataset']['kwargs']['segments']['test'][1]

    # 时间范围信息
    y_position = 500
    
    # 训练时间范围
    train_info = "训练时间范围:  " + train_start.strftime("%Y-%m-%d") + " 至 " + train_end.strftime("%Y-%m-%d")
    draw.text((200, y_position), train_info, fill='black', font=content_font)
    y_position += 100
    
    # 验证时间范围
    valid_info = "验证时间范围:  " + valid_start.strftime("%Y-%m-%d") + " 至 " + valid_end.strftime("%Y-%m-%d")
    draw.text((200, y_position), valid_info, fill='black', font=content_font)
    y_position += 100
    
    # 测试时间范围
    test_info = "测试时间范围:  " + test_start.strftime("%Y-%m-%d") + " 至 " + test_end.strftime("%Y-%m-%d")
    draw.text((200, y_position), test_info, fill='black', font=content_font)
    y_position += 100
    
    market = config['market']
    if market == 'csi500':
        dataset_info = "股票池: 中证500"
    elif market == 'csi300':
        dataset_info = "股票池: 沪深300"
    elif market == 'csi50':
        dataset_info = "股票池: 上证50"
    elif market == 'csi2000':
        dataset_info = "股票池: 中证2000"
    else:
        dataset_info = "股票池: " + market

    benchmark = config['benchmark']
    if benchmark == '000905.sh':
        benchmark_info = "基准指数: 中证500"
    elif benchmark == '000300.sh':
        benchmark_info = "基准指数: 沪深300"
    elif benchmark == '000016.sh':
        benchmark_info = "基准指数: 上证50"
    elif benchmark == '000852.sh':
        benchmark_info = "基准指数: 中证2000"
    else:
        benchmark_info = "基准指数: " + benchmark

    # 数据集信息
    draw.text((200, y_position), dataset_info, fill='black', font=content_font)
    y_position += 100
    
    # 基准信息
    draw.text((200, y_position), benchmark_info, fill='black', font=content_font)
    y_position += 100
    
    # 回测结果表格
    y_position += 100

    
    analysis_df = recorder.load_object("portfolio_analysis/port_analysis_1day.pkl")

    an_return_wo_cost = analysis_df.loc["excess_return_without_cost", "annualized_return"]
    an_return_wo_cost = an_return_wo_cost.values[0]
    an_return_wo_cost = f"{an_return_wo_cost:.2%}"
    an_return_wo_cost = str(an_return_wo_cost)

    an_return_w_cost = analysis_df.loc["excess_return_with_cost", "annualized_return"]
    an_return_w_cost = an_return_w_cost.values[0]
    an_return_w_cost = f"{an_return_w_cost:.2%}"
    an_return_w_cost = str(an_return_w_cost)

    ir_without_cost = analysis_df.loc["excess_return_without_cost", "information_ratio"]
    ir_without_cost = ir_without_cost.values[0]
    ir_without_cost = f"{ir_without_cost:.2f}"
    ir_without_cost = str(ir_without_cost)

    ir_with_cost = analysis_df.loc["excess_return_with_cost", "information_ratio"]
    ir_with_cost = ir_with_cost.values[0]
    ir_with_cost = f"{ir_with_cost:.2f}"
    ir_with_cost = str(ir_with_cost)

    max_drawdown_without_cost = analysis_df.loc["excess_return_without_cost", "max_drawdown"]
    max_drawdown_without_cost = max_drawdown_without_cost.values[0] 
    max_drawdown_without_cost = f"{max_drawdown_without_cost:.2%}"
    max_drawdown_without_cost = str(max_drawdown_without_cost)

    max_drawdown_with_cost = analysis_df.loc["excess_return_with_cost", "max_drawdown"]
    max_drawdown_with_cost = max_drawdown_with_cost.values[0]
    max_drawdown_with_cost = f"{max_drawdown_with_cost:.2%}"
    max_drawdown_with_cost = str(max_drawdown_with_cost)

    benchmark_report = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
    bench_return = benchmark_report['bench']

    
    max_drawdown = (bench_return.cumsum() - bench_return.cumsum().cummax()).min()
    max_drawdown = f"{max_drawdown:.2%}"
    max_drawdown = str(max_drawdown)

    ir = bench_return.mean() / bench_return.std() * np.sqrt(252)
    ir = f"{ir:.2f}"
    ir = str(ir)

    returns = bench_return.mean() * 252
    returns = f"{returns:.2%}"
    returns = str(returns)


    # 绘制表格
    table_data = [
        ["回测结果", "基准指数", "策略(无成本)", "策略(含成本)"],
        ["年化超额收益率", returns, an_return_wo_cost, an_return_w_cost],
        ["信息比率", ir, ir_without_cost, ir_with_cost],
        ["最大回撤", max_drawdown, max_drawdown_without_cost, max_drawdown_with_cost]
    ]
    
    # 表格参数
    table_x = 200
    table_y = y_position
    cell_width = 400
    cell_height = 80
    row_count = len(table_data)
    col_count = len(table_data[0])
    
    # 绘制表格边框和内容
    for row in range(row_count):
        for col in range(col_count):
            # 计算单元格位置
            x = table_x + col * cell_width
            y = table_y + row * cell_height
            
            # 绘制单元格边框
            draw.rectangle([x, y, x + cell_width, y + cell_height], outline='black', width=2)
            
            # 设置单元格背景色
            if row == 0:  # 表头
                draw.rectangle([x+2, y+2, x + cell_width-2, y + cell_height-2], fill='lightgray')
            
            # 添加文本
            text = table_data[row][col]
            # 计算文本居中位置
            text_bbox = draw.textbbox((0, 0), text, font=content_font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]
            text_x = x + (cell_width - text_width) // 2
            text_y = y + (cell_height - text_height) // 2
            
            draw.text((text_x, text_y), text, fill='black', font=content_font)
    
    y_position = table_y + row_count * cell_height 
    
    # 生成时间
    y_position += 50
    current_time = datetime.now().strftime("%Y-%m-%d")
    time_info = f"报告生成时间: {current_time}"
    draw.text((200, y_position), time_info, fill='gray', font=content_font)
    
    # 在第一页添加第一张图片（在报告生成时间下面）
    figure_files = glob.glob(os.path.join(image_path, "figure_*.png"))
    if figure_files:
        figure_files.sort()
        
        first_image_path = figure_files[0]
        try:
            first_img = Image.open(first_image_path)
            if first_img.mode != 'RGB':
                first_img = first_img.convert('RGB')
            
            # 计算图片放置位置（在报告生成时间下面，但往上移动）
            img_y = y_position + 50  # 在报告生成时间下方50像素（减少了50像素）
            img_x = 200  # 从左侧开始，与文字对齐
            available_width = page_width - img_x - 200  # 可用宽度
            available_height = page_height - img_y - 200  # 可用高度
            
            # 调整图片尺寸到精确尺寸
            first_img_resized = resize_image_to_exact_size(first_img, available_width, available_height)
            
            # 粘贴图片
            img.paste(first_img_resized, (img_x, img_y))
         
            
        except Exception as e:
            log.exception(f"添加第一张图片失败: {e}")
    
    return img

def resize_image_to_fit(img, target_width, target_height):
   
    original_width, original_height = img.size
    
    width_ratio = target_width / original_width
    height_ratio = target_height / original_height
    

    scale_ratio = min(width_ratio, height_ratio)
    
    new_width = int(original_width * scale_ratio)
    new_height = int(original_height * scale_ratio)
    
    img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    
    return img_resized

def resize_image_to_exact_size(img, target_width, target_height):
   
   
    original_width, original_height = img.size
    
    width_ratio = target_width / original_width
    height_ratio = target_height / original_height

    scale_ratio = min(width_ratio, height_ratio)
    
    new_width = int(original_width * scale_ratio)
    new_height = int(original_height * scale_ratio)
    
    img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
    
    final_img = Image.new('RGB', (target_width, target_height), 'white')
    
    x_offset = (target_width - new_width) // 2
    y_offset = (target_height - new_height) // 2
    
    final_img.paste(img_resized, (x_offset, y_offset))
    
    return final_img

def create_multi_image_page(images, page_width, page_height, images_per_page=2):
    
    page_img = Image.new('RGB', (page_width, page_height), 'white')
    
   
    margin = 100  
    available_width = page_width - 2 * margin
    available_height = page_height - 2 * margin
    
    if images_per_page == 2:
    
        img_width = available_width
        img_height = (available_height - margin) // 2
    elif images_per_page == 4:
  
        img_width = (available_width - margin) // 2
        img_height = (available_height - margin) // 2
    else:
       
        img_width = available_width
        img_height = available_height
    
    for i, img in enumerate(images):
        if i >= images_per_page:
            break
            
        img_resized = resize_image_to_exact_size(img, img_width, img_height)
        
    
        if images_per_page == 2:
           
            x = margin
            y = margin + i * (img_height + margin)
        elif images_per_page == 4:
           
            row = i // 2
            col = i % 2
            x = margin + col * (img_width + margin)
            y = margin + row * (img_height + margin)
        else:
           
            x = (page_width - img_resized.width) // 2
            y = (page_height - img_resized.height) // 2
        
        page_img.paste(img_resized, (x, y))
    
    return page_img

def export_figures_to_pdf(experiment_id, experiment_name, provider_uri, image_path):
   
    figure_files = glob.glob(os.path.join(image_path, "figure_*.png"))
    if not figure_files:
        log.warning("未找到任何图片")
        return
    figure_files.sort()
    global recorder
    global_init(experiment_id, experiment_name, provider_uri)
    info_page = create_info_page_with_first_image(experiment_id, experiment_name,provider_uri, image_path)
    split_files = [
        os.path.join(image_path, "split_importance_test.png"),
        os.path.join(image_path, "split_importance_valid.png"),
        os.path.join(image_path, "gain_importance_test.png"),
        os.path.join(image_path, "gain_importance_valid.png")
    ]
    shape_files = [
        os.path.join(image_path, "shap_bar_test.png"),
        os.path.join(image_path, "shap_bar_valid.png"),
        os.path.join(image_path, "shap_summary_test.png"),
        os.path.join(image_path, "shap_summary_valid.png")
    ]
    figure_files = glob.glob(os.path.join(image_path, "figure_*.png"))

    if not figure_files:
        print("未找到任何figure_*.png文件")
        return
    
    figure_files.sort()
    
    
    global recorder
    global_init(experiment_id, experiment_name, provider_uri)
    info_page = create_info_page_with_first_image(experiment_id, experiment_name, provider_uri, image_path)
    
    page_width = 2100  
    page_height = 2970
    images_per_page = 2  
   
    loaded_images = []
    for i, file in enumerate(figure_files):
        if i == 0:  
            continue
        try:
            img = Image.open(file)
       
            if img.mode != 'RGB':
                img = img.convert('RGB')
            loaded_images.append(img)
            
        except Exception as e:
            log.exception(f"加载图片失败 {file}: {e}")
    
    pages = [info_page]  

    # 将剩余图片分组并创建页面
    for i in range(0, len(loaded_images), images_per_page):
        page_images = loaded_images[i:i + images_per_page]
        page = create_multi_image_page(page_images, page_width, page_height, images_per_page)
        pages.append(page)


    split_files = [
        os.path.join(image_path, "split_importance_test.png"),
        os.path.join(image_path, "split_importance_valid.png"),
        os.path.join(image_path, "gain_importance_test.png"),
        os.path.join(image_path, "gain_importance_valid.png")
    ]
    split_imgs = []
    for split_file in split_files:
        if os.path.exists(split_file):
            try:
                img = Image.open(split_file)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                split_imgs.append(img)
            except Exception as e:
                log.exception(f"加载分组重要性图片失败 {split_file}: {e}")

    for i in range(0, len(split_imgs), 2):
        page_imgs = split_imgs[i:i+2]
        split_page = create_multi_image_page(page_imgs, page_width, page_height, images_per_page=2)
        pages.append(split_page)
    shape_files = [
        os.path.join(image_path, "shap_bar_test.png"),
        os.path.join(image_path, "shap_bar_valid.png"),
        os.path.join(image_path, "shap_summary_test.png"),
        os.path.join(image_path, "shap_summary_valid.png")
    ]
    shape_imgs = []
    for shape_file in shape_files:
        if os.path.exists(shape_file):
            try:
                img = Image.open(shape_file)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                shape_imgs.append(img)
            except Exception as e:
                log.exception(f"加载形状散点图失败 {shape_file}: {e}")

    for i in range(0, len(shape_imgs), 1):
        page_imgs = shape_imgs[i:i+1]
        shape_page = create_multi_image_page(page_imgs, page_width, page_height, images_per_page=1)
        pages.append(shape_page)

    if len(pages) <= 1:
        log.error("没有创建任何图片页面")
        return

    # 保存为PDF
    config = recorder.load_object("config")
    model_type = config['task']['model']['class']
    output_filename = f"{model_type}报告1.pdf"
    try:
        pages[0].save(
            output_filename,
            "PDF",
            resolution=100.0,
            save_all=True,
            append_images=pages[1:]
        )
        log.info(f"\n成功导出PDF文件: {output_filename}")
    except Exception as e:
        log.error(f"导出PDF失败: {e}")

if __name__ == "__main__":
    experiment_id = "505608931795866282"
    experiment_name = "test_lgbm"
    image_path = "E:\\qlib_data\\analysis_figures"

    provider_uri = r"E:\qlib_data\tushare_qlib_data\qlib_bin"
    export_figures_to_pdf(experiment_id, experiment_name, provider_uri, image_path)
