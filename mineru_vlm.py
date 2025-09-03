# Copyright (c) Opendatalab. All rights reserved.
import copy
import json
import os
import logging
from pathlib import Path
from datetime import datetime

from loguru import logger

from mineru.cli.common import convert_pdf_bytes_to_bytes_by_pypdfium2, prepare_env, read_fn
from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.utils.draw_bbox import draw_layout_bbox
from mineru.utils.enum_class import MakeMode
from mineru.backend.vlm.vlm_analyze import doc_analyze as vlm_doc_analyze
from mineru.backend.vlm.vlm_middle_json_mkcontent import union_make as vlm_union_make
from mineru.utils.models_download_utils import auto_download_and_get_model_root_path


def setup_logging(log_dir="logs"):
    """
    设置日志配置，将mineru的日志输出到文件
    """
    # 创建logs目录
    os.makedirs(log_dir, exist_ok=True)
    
    # 生成带时间戳的日志文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"mineru_parse_{timestamp}.log")
    
    # 配置loguru，将日志输出到文件和控制台
    logger.remove()  # 移除默认的处理器
    logger.add(log_file, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
    logger.add(lambda msg: print(msg, end=""), level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
    
    logger.info(f"日志文件已创建: {log_file}")
    return log_file


def do_parse(
    output_dir,  # 输出目录，用于存储解析结果
    pdf_file_names: list[str],  # PDF文件名列表
    pdf_bytes_list: list[bytes],  # PDF字节数据列表
    p_lang_list: list[str],  # 每个PDF的语言列表，默认为'en'
    backend="vlm-transformers",  # 解析PDF的后端，使用vlm-transformers加速
    formula_enable=True,  # 启用公式解析
    table_enable=True,  # 启用表格解析
    server_url=None,  # VLM后端服务器URL（此处不使用）
    f_draw_layout_bbox=True,  # 是否绘制布局边界框
    f_dump_md=True,  # 是否输出markdown文件
    f_dump_middle_json=True,  # 是否输出中间JSON文件
    f_dump_model_output=True,  # 是否输出模型输出文件
    f_dump_orig_pdf=True,  # 是否输出原始PDF文件
    f_dump_content_list=True,  # 是否输出内容列表文件
    f_make_md_mode=MakeMode.MM_MD,  # 制作markdown内容的模式
    start_page_id=0,  # 开始页面ID
    end_page_id=None,  # 结束页面ID
):
    """
    执行PDF解析的主要函数
    使用vlm-transformers后端进行加速解析
    """
    
    logger.info(f"开始解析 {len(pdf_file_names)} 个PDF文件")
    logger.info(f"使用后端: {backend}")
    logger.info(f"输出目录: {output_dir}")
    
    # 使用vlm-transformers后端进行解析
    if backend == "vlm-transformers":
        logger.info("使用VLM-Transformers后端进行解析...")
        
        # 遍历每个PDF文件进行解析
        for idx, pdf_bytes in enumerate(pdf_bytes_list):
            pdf_file_name = pdf_file_names[idx]
            logger.info(f"正在解析文件 {idx+1}/{len(pdf_file_names)}: {pdf_file_name}")
            
            try:
                # 转换PDF字节数据（如果指定了页面范围）
                if start_page_id > 0 or end_page_id is not None:
                    pdf_bytes = convert_pdf_bytes_to_bytes_by_pypdfium2(pdf_bytes, start_page_id, end_page_id)
                    logger.info(f"已转换PDF页面范围: {start_page_id} 到 {end_page_id if end_page_id else '末尾'}")
                
                # 准备输出环境
                local_image_dir, local_md_dir = prepare_env(output_dir, pdf_file_name, "vlm")
                image_writer, md_writer = FileBasedDataWriter(local_image_dir), FileBasedDataWriter(local_md_dir)
                
                logger.info(f"图片输出目录: {local_image_dir}")
                logger.info(f"文档输出目录: {local_md_dir}")
                
                # 使用VLM进行文档分析
                logger.info("开始VLM文档分析...")
                middle_json, infer_result = vlm_doc_analyze(
                    pdf_bytes, 
                    image_writer=image_writer, 
                    backend="transformers",  # 使用transformers后端
                    server_url=server_url
                )
                logger.info("VLM文档分析完成")
                
                # 获取PDF信息
                pdf_info = middle_json["pdf_info"]
                logger.info(f"PDF信息提取完成，共 {len(pdf_info)} 页")
                
                # 绘制布局边界框（如果启用）
                if f_draw_layout_bbox:
                    logger.info("正在绘制布局边界框...")
                    draw_layout_bbox(pdf_info, pdf_bytes, local_md_dir, f"{pdf_file_name}_layout.pdf")
                    logger.info("布局边界框绘制完成")
                                
                # 保存原始PDF文件（如果启用）
                if f_dump_orig_pdf:
                    logger.info("正在保存原始PDF文件...")
                    md_writer.write(f"{pdf_file_name}_origin.pdf", pdf_bytes)
                    logger.info("原始PDF文件保存完成")
                
                # 生成并保存Markdown文件（如果启用）
                if f_dump_md:
                    logger.info("正在生成Markdown文件...")
                    image_dir = str(os.path.basename(local_image_dir))
                    md_content_str = vlm_union_make(pdf_info, f_make_md_mode, image_dir)
                    md_writer.write_string(f"{pdf_file_name}.md", md_content_str)
                    logger.info("Markdown文件生成完成")
                
                # 生成并保存内容列表（如果启用）
                if f_dump_content_list:
                    logger.info("正在生成内容列表...")
                    image_dir = str(os.path.basename(local_image_dir))
                    content_list = vlm_union_make(pdf_info, MakeMode.CONTENT_LIST, image_dir)
                    md_writer.write_string(
                        f"{pdf_file_name}_content_list.json",
                        json.dumps(content_list, ensure_ascii=False, indent=4),
                    )
                    logger.info("内容列表生成完成")
                
                # 保存中间JSON文件（如果启用）
                if f_dump_middle_json:
                    logger.info("正在保存中间JSON文件...")
                    md_writer.write_string(
                        f"{pdf_file_name}_middle.json",
                        json.dumps(middle_json, ensure_ascii=False, indent=4),
                    )
                    logger.info("中间JSON文件保存完成")
                
                # 保存模型输出（如果启用）
                if f_dump_model_output:
                    logger.info("正在保存模型输出...")
                    model_output = ("\n" + "-" * 50 + "\n").join(infer_result)
                    md_writer.write_string(f"{pdf_file_name}_model_output.txt", model_output)
                    logger.info("模型输出保存完成")
                
                logger.info(f"文件 {pdf_file_name} 解析完成，输出目录: {local_md_dir}")
                
            except Exception as e:
                logger.error(f"解析文件 {pdf_file_name} 时发生错误: {str(e)}")
                continue
    
    else:
        logger.error(f"不支持的backend: {backend}，请使用 'vlm-transformers'")


def parse_doc(
    path_list: list[Path],
    output_dir,
    lang="en",
    backend="vlm-transformers",  # 默认使用vlm-transformers
    server_url=None,
    start_page_id=0,
    end_page_id=None,
    batch_size=5,  # 批处理大小，默认5个文件
    # 新增：所有do_parse函数的参数
    formula_enable=True,  # 启用公式解析
    table_enable=True,  # 启用表格解析
    f_draw_layout_bbox=True,  # 是否绘制布局边界框
    f_dump_md=True,  # 是否输出markdown文件
    f_dump_middle_json=True,  # 是否输出中间JSON文件
    f_dump_model_output=True,  # 是否输出模型输出文件
    f_dump_orig_pdf=True,  # 是否输出原始PDF文件
    f_dump_content_list=True,  # 是否输出内容列表文件
    f_make_md_mode=MakeMode.MM_MD,  # 制作markdown内容的模式
):
    """
    解析文档的主函数（批处理版本）
    
    参数说明:
    path_list: 要解析的文档路径列表，可以是PDF或图片文件
    output_dir: 存储解析结果的输出目录
    lang: 语言选项，默认为'en'
    backend: 解析PDF的后端，只支持vlm-transformers
    server_url: 不使用sglang，此参数保留但不使用
    start_page_id: 开始解析的页面ID，默认为0
    end_page_id: 结束解析的页面ID，默认为None（解析到文档末尾）
    batch_size: 批处理大小，每次处理多少个文件，默认为5
    
    # 新增的解析控制参数:
    formula_enable: 启用公式解析，默认True
    table_enable: 启用表格解析，默认True
    f_draw_layout_bbox: 是否绘制布局边界框，默认True
    f_dump_md: 是否输出markdown文件，默认True
    f_dump_middle_json: 是否输出中间JSON文件，默认True
    f_dump_model_output: 是否输出模型输出文件，默认True
    f_dump_orig_pdf: 是否输出原始PDF文件，默认True
    f_dump_content_list: 是否输出内容列表文件，默认True
    f_make_md_mode: 制作markdown内容的模式，默认MM_MD
    """
    try:
        logger.info(f"开始批量解析文档，共 {len(path_list)} 个文件")
        logger.info(f"批处理大小: {batch_size}")
        logger.info(f"输出目录: {output_dir}")
        logger.info(f"语言设置: {lang}")
        logger.info(f"页面范围: {start_page_id} 到 {end_page_id if end_page_id else '末尾'}")
        
        # 打印所有解析参数
        logger.info("=== 解析参数配置 ===")
        logger.info(f"公式解析: {'启用' if formula_enable else '禁用'}")
        logger.info(f"表格解析: {'启用' if table_enable else '禁用'}")
        logger.info(f"绘制布局边界框: {'启用' if f_draw_layout_bbox else '禁用'}")
        logger.info(f"输出Markdown: {'启用' if f_dump_md else '禁用'}")
        logger.info(f"输出中间JSON: {'启用' if f_dump_middle_json else '禁用'}")
        logger.info(f"输出模型输出: {'启用' if f_dump_model_output else '禁用'}")
        logger.info(f"输出原始PDF: {'启用' if f_dump_orig_pdf else '禁用'}")
        logger.info(f"输出内容列表: {'启用' if f_dump_content_list else '禁用'}")
        logger.info(f"Markdown模式: {f_make_md_mode}")
        logger.info("==================")
        
        # 分批处理文件
        total_batches = (len(path_list) + batch_size - 1) // batch_size
        logger.info(f"将分 {total_batches} 批进行处理")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(path_list))
            current_batch = path_list[start_idx:end_idx]
            
            logger.info(f"开始处理第 {batch_idx + 1}/{total_batches} 批，文件 {start_idx + 1}-{end_idx}")
            
            # 准备当前批次的文件
            file_name_list = []
            pdf_bytes_list = []
            lang_list = []
            
            for path in current_batch:
                file_name = str(Path(path).stem)
                logger.info(f"准备解析文件: {file_name}")
                
                try:
                    pdf_bytes = read_fn(path)
                    file_name_list.append(file_name)
                    pdf_bytes_list.append(pdf_bytes)
                    lang_list.append(lang)
                    logger.info(f"文件 {file_name} 读取成功，大小: {len(pdf_bytes)} 字节")
                except Exception as e:
                    logger.error(f"读取文件 {file_name} 失败: {str(e)}")
                    continue
            
            if not file_name_list:
                logger.warning(f"第 {batch_idx + 1} 批没有成功读取的文件，跳过")
                continue
            
            logger.info(f"第 {batch_idx + 1} 批成功准备 {len(file_name_list)} 个文件进行解析")
            
            # 执行当前批次的解析，传递所有参数
            do_parse(
                output_dir=output_dir,
                pdf_file_names=file_name_list,
                pdf_bytes_list=pdf_bytes_list,
                p_lang_list=lang_list,
                backend=backend,
                formula_enable=formula_enable,
                table_enable=table_enable,
                server_url=server_url,
                f_draw_layout_bbox=f_draw_layout_bbox,
                f_dump_md=f_dump_md,
                f_dump_middle_json=f_dump_middle_json,
                f_dump_model_output=f_dump_model_output,
                f_dump_orig_pdf=f_dump_orig_pdf,
                f_dump_content_list=f_dump_content_list,
                f_make_md_mode=f_make_md_mode,
                start_page_id=start_page_id,
                end_page_id=end_page_id
            )
            
            # 清理当前批次的内存
            del file_name_list, pdf_bytes_list, lang_list
            logger.info(f"第 {batch_idx + 1} 批处理完成，已清理内存")
            
            # 可选：在批次之间添加短暂休息，避免系统过载
            if batch_idx < total_batches - 1:
                logger.info("等待2秒后处理下一批...")
                import time
                time.sleep(2)
        
        logger.info("所有批次处理完成！")
        
    except Exception as e:
        logger.exception(f"解析过程中发生错误: {str(e)}")