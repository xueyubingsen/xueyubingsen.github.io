import pandas as pd
import json
import re

def process_and_expand_edges(input_excel_path, output_json_path):
    """
    处理Excel数据，展开source和target列中的逗号分隔值，生成G6图所需的edges数据
    
    参数:
    input_excel_path (str): 输入的Excel文件路径
    output_json_path (str): 输出的JSON文件路径
    """
    try:
        # 读取Sheet1（nodes信息）
        nodes_df = pd.read_excel(input_excel_path, sheet_name=0)
        
        # 1. 处理NaN值：将所有NaN替换为空字符串
        nodes_df = nodes_df.fillna('')
        
        # 2. 处理换行符：将所有字符串列中的换行符替换为<br>
        # def replace_newlines_with_br(df):
        #     for column in df.columns:
        #         if df[column].dtype == 'object':  # 对象类型通常是字符串
        #             df[column] = df[column].apply(
        #                 lambda x: re.sub(r'\r\n|\r|\n', '<br>', str(x)) if pd.notna(x) and x != '' else x
        #             )
        #     return df

        def replace_newlines_with_br(df):
            """
            只处理details列中的换行符，将其替换为<br>
            
            参数:
            df (DataFrame): 要处理的DataFrame
            
            返回:
            DataFrame: 处理后的DataFrame
            """
            # 检查DataFrame中是否存在details列
            if 'details' in df.columns:
                # 只对details列进行处理
                df['details'] = df['details'].apply(
                    lambda x: re.sub(r'\r\n|\r|\n', '<br>', str(x)) if pd.notna(x) and x != '' else x
                )
            else:
                print("警告: DataFrame中不存在'details'列，跳过换行符处理")
            
            return df

        
        nodes_df = replace_newlines_with_br(nodes_df)
        
        # 准备收集所有边的列表
        edges_list = []
        
        # 处理每一行数据
        for index, row in nodes_df.iterrows():
            current_id = str(row['id']).strip() if row['id'] else ''
            
            # 处理source列：拆分成多个source，每个source连接到当前id
            if 'source' in row and row['source'] and row['source'] != '':
                sources = str(row['source']).split(',')
                for source in sources:
                    source_clean = source.strip()
                    if source_clean and source_clean != current_id:  # 避免自环
                        edges_list.append({
                            'source': source_clean,
                            'target': current_id
                        })
            
            # 处理target列：拆分成多个target，当前id连接到每个target
            if 'target' in row and row['target'] and row['target'] != '':
                targets = str(row['target']).split(',')
                for target in targets:
                    target_clean = target.strip()
                    if target_clean and target_clean != current_id:  # 避免自环
                        edges_list.append({
                            'source': current_id,
                            'target': target_clean
                        })
        
        # 去重：避免因为数据重复而产生相同的边
        edges_df = pd.DataFrame(edges_list)
        edges_df = edges_df.drop_duplicates()
        
        # 对edges数据也进行NaN和换行符处理
        edges_df = edges_df.fillna('')
        edges_df = replace_newlines_with_br(edges_df)
        
        # 构建完整的G6图数据
        graph_data = {
            "nodes": nodes_df.to_dict('records'),
            "edges": edges_df.to_dict('records')
        }
        
        # 保存为JSON文件
        with open(output_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(graph_data, json_file, ensure_ascii=False, indent=2)
        
        print(f"处理完成！JSON文件已保存至: {output_json_path}")
        print(f"原始节点数量: {len(nodes_df)}")
        print(f"生成的边数量: {len(edges_df)}")
        
        # 显示处理摘要
        print("\n处理摘要:")
        print(f"- NaN值已替换为空字符串")
        print(f"- 换行符已统一替换为<br>标签")
        print(f"- 展开的边关系已去重")
        
        # 显示一些统计信息
        print("\n边关系统计:")
        edge_counts = edges_df.groupby('source').size().sort_values(ascending=False)
        print("连接最多的前10个source节点:")
        print(edge_counts.head(10))
        
        return graph_data
        
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        return None

# 使用示例
if __name__ == "__main__":
    input_file = "/Users/junzhang/Downloads/参数、指标、页面、报错、笔记.xlsx"
    output_file = "/Users/junzhang/Downloads/expanded_graph_data2.json"
    
    result = process_and_expand_edges(input_file, output_file)
