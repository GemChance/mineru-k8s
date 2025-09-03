#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mineru PDF批量解析主程序
支持按子文件夹分组处理，并按时间段倒序排序
"""

import os
import re
from pathlib import Path
from datetime import datetime
from loguru import logger

from mineru_vlm import setup_logging, parse_doc
from mineru.utils.enum_class import MakeMode


def extract_date_from_folder_name(folder_name: str) -> datetime:
    """
    从文件夹名称中提取日期信息
    文件夹格式: 2015-01-01_2015-03-31_MNSC
    返回第一个日期作为排序依据
    """
    try:
        # 使用正则表达式匹配日期格式 YYYY-MM-DD
        date_pattern = r'(\d{4}-\d{2}-\d{2})'
        dates = re.findall(date_pattern, folder_name)
        
        if dates:
            # 取第一个日期进行排序
            first_date = datetime.strptime(dates[0], '%Y-%m-%d')
            return first_date
        
        # 如果没有找到日期，返回一个很早的日期（将排在最后）
        logger.warning(f"无法从文件夹名 {folder_name} 中提取日期，将排在最后")
        return datetime(1900, 1, 1)
        
    except Exception as e:
        logger.error(f"提取日期时发生错误: {str(e)}")
        return datetime(1900, 1, 1)


def sort_folders_by_date_desc(folder_paths: list[Path]) -> list[Path]:
    """
    按日期倒序排序文件夹路径
    最新日期的文件夹排在前面
    """
    # 创建(文件夹路径, 日期)的元组列表
    folder_date_pairs = []
    for path in folder_paths:
        folder_name = path.name
        date = extract_date_from_folder_name(folder_name)
        folder_date_pairs.append((path, date))
    
    # 按日期倒序排序
    folder_date_pairs.sort(key=lambda x: x[1], reverse=True)
    
    # 提取排序后的文件夹路径
    sorted_paths = [pair[0] for pair in folder_date_pairs]
    
    # 打印排序结果
    logger.info("=== 文件夹按日期倒序排序结果 ===")
    for i, (path, date) in enumerate(folder_date_pairs):
        if date.year > 1900:
            logger.info(f"{i+1:2d}. {path.name} (日期: {date.strftime('%Y-%m-%d')})")
        else:
            logger.info(f"{i+1:2d}. {path.name} (日期: 未知)")
    logger.info("==================================")
    
    return sorted_paths


def process_folder_structure(
    input_root_dir: str,
    output_root_dir: str,
    batch_size: int = 5,
    **parsing_config
):
    """
    处理文件夹结构，按子文件夹分组处理PDF文件
    
    参数:
    input_root_dir: 输入根目录（包含子文件夹）
    output_root_dir: 输出根目录
    batch_size: 批处理大小
    **parsing_config: 解析配置参数
    """
    input_path = Path(input_root_dir)
    output_path = Path(output_root_dir)
    
    if not input_path.exists():
        logger.error(f"输入目录不存在: {input_root_dir}")
        return
    
    # 创建输出根目录
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 获取所有子文件夹
    subfolders = [f for f in input_path.iterdir() if f.is_dir()]
    
    if not subfolders:
        logger.warning(f"在 {input_root_dir} 中没有找到子文件夹")
        return
    
    logger.info(f"找到 {len(subfolders)} 个子文件夹")
    
    # 按日期倒序排序文件夹
    sorted_subfolders = sort_folders_by_date_desc(subfolders)
    
    # 统计信息
    total_pdfs = 0
    total_skipped = 0
    total_processed = 0
    skipped_folders = 0
    
    # 处理每个子文件夹
    for folder_idx, subfolder in enumerate(sorted_subfolders):
        logger.info(f"开始处理文件夹 {folder_idx + 1}/{len(sorted_subfolders)}: {subfolder.name}")
        
        # 创建对应的输出子目录
        output_subfolder = output_path / subfolder.name
        output_subfolder.mkdir(exist_ok=True)
        
        # 查找当前子文件夹中的所有PDF文件
        pdf_files = list(subfolder.glob('*.pdf'))
        
        if not pdf_files:
            logger.warning(f"文件夹 {subfolder.name} 中没有找到PDF文件，跳过")
            continue
        
        total_pdfs += len(pdf_files)
        logger.info(f"文件夹 {subfolder.name} 中找到 {len(pdf_files)} 个PDF文件")
        
        # 检查已解析的结果，过滤掉已经处理过的PDF文件
        # 注意：解析结果存储在 {pdf_name}/vlm/ 子文件夹中
        filtered_pdf_files = []
        skipped_count = 0
        
        for pdf_file in pdf_files:
            # 获取PDF文件名（无后缀）
            pdf_name_without_ext = pdf_file.stem
            
            # 检查输出目录中是否已经存在对应的解析结果文件夹
            output_pdf_dir = output_subfolder / pdf_name_without_ext
            
            if output_pdf_dir.exists() and output_pdf_dir.is_dir():
                # 检查vlm子文件夹中是否包含解析结果文件
                vlm_dir = output_pdf_dir / 'vlm'
                has_results = False
                
                if vlm_dir.exists() and vlm_dir.is_dir():
                    # 检查vlm子文件夹中的解析结果文件
                    for result_file in vlm_dir.iterdir():
                        if result_file.is_file() and result_file.suffix in ['.md', '.json', '.txt']:
                            has_results = True
                            break
                else:
                    # vlm子文件夹不存在，记录调试信息
                    logger.debug(f"  - {pdf_name_without_ext} 的vlm子文件夹不存在: {vlm_dir}")
                
                if has_results:
                    logger.info(f"  ✓ {pdf_name_without_ext} 已解析完成（vlm子文件夹中有结果文件），跳过")
                    skipped_count += 1
                    continue
                else:
                    logger.info(f"  ? {pdf_name_without_ext} 输出目录存在但vlm子文件夹中无结果文件，将重新解析")
            
            filtered_pdf_files.append(pdf_file)
        
        if skipped_count > 0:
            logger.info(f"文件夹 {subfolder.name} 中跳过 {skipped_count} 个已解析的PDF文件")
            total_skipped += skipped_count
        
        if not filtered_pdf_files:
            logger.info(f"文件夹 {subfolder.name} 中所有PDF文件都已解析完成，跳过整个文件夹")
            skipped_folders += 1
            continue
        
        logger.info(f"文件夹 {subfolder.name} 中需要解析 {len(filtered_pdf_files)} 个PDF文件")
        
        # 使用mineru_vlm解析当前文件夹的PDF文件
        try:
            parse_doc(
                path_list=filtered_pdf_files,
                output_dir=str(output_subfolder),
                batch_size=batch_size,
                **parsing_config
            )
            total_processed += len(filtered_pdf_files)
            logger.info(f"文件夹 {subfolder.name} 处理完成")
            
        except Exception as e:
            logger.error(f"处理文件夹 {subfolder.name} 时发生错误: {str(e)}")
            continue
        
        # 在文件夹之间添加短暂休息
        if folder_idx < len(sorted_subfolders) - 1:
            logger.info("等待3秒后处理下一个文件夹...")
            import time
            time.sleep(3)
    
    # 输出最终统计信息
    logger.info("=== 处理完成统计 ===")
    logger.info(f"总PDF文件数: {total_pdfs}")
    logger.info(f"已跳过文件数: {total_skipped}")
    logger.info(f"实际处理文件数: {total_processed}")
    logger.info(f"跳过的文件夹数: {skipped_folders}")
    logger.info(f"处理的文件夹数: {len(sorted_subfolders) - skipped_folders}")
    logger.info("====================")
    
    logger.info("所有文件夹处理完成！")


def main():
    """主函数"""
    # 设置日志
    log_file = setup_logging("testoutput/logs")
    logger.info("=== Mineru PDF批量解析工具启动 ===")
    
    # 配置路径
    input_root_dir = "/data/DownloadsPaper"  # 输入根目录
    output_root_dir = "/data/output"         # 输出根目录
    
    logger.info(f"输入根目录: {input_root_dir}")
    logger.info(f"输出根目录: {output_root_dir}")
    
    # 配置批处理参数
    BATCH_SIZE = 5  # 每批处理5个文件
    
    # 配置解析参数
    PARSING_CONFIG = {
        'formula_enable': True,        # 是否启用公式解析
        'table_enable': True,          # 是否启用表格解析
        'f_draw_layout_bbox': True,   # 是否绘制布局边界框
        'f_dump_md': True,            # 是否输出markdown文件
        'f_dump_middle_json': True,   # 是否输出中间JSON文件
        'f_dump_model_output': True,  # 是否输出模型输出文件
        'f_dump_orig_pdf': False,     # 是否输出原始PDF文件
        'f_dump_content_list': True,  # 是否输出内容列表文件
        'f_make_md_mode': MakeMode.MM_MD,  # markdown模式
    }
    
    logger.info(f"批处理大小: {BATCH_SIZE}")
    logger.info("解析参数配置:")
    for key, value in PARSING_CONFIG.items():
        logger.info(f"  {key}: {value}")
    
    # 开始处理文件夹结构
    logger.info("开始按文件夹结构处理PDF文件...")
    process_folder_structure(
        input_root_dir=input_root_dir,
        output_root_dir=output_root_dir,
        batch_size=BATCH_SIZE,
        **PARSING_CONFIG
    )
    
    logger.info("=== 所有处理完成 ===")
    logger.info(f"详细日志请查看: {log_file}")


if __name__ == '__main__':
    main()