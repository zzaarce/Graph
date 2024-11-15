import mysql.connector
import json
from openai import OpenAI
from neo4j import GraphDatabase
import ast  # 用于将字符串转换为 Python 对象
from datetime import datetime  # 用于获取当前日期
import ast
import os
from openai import AzureOpenAI



xxx = {
    # 领导、职位相关
    "leader": "LEADER",  # 领导
    "member": "MEMBER",  # 成员
    "professor": "PROFESSOR",  # 教授
    "president": "PRESIDENT",  # 总统
    "organization": "ORGANIZATION",  # 组织
    "works at": "WORKS_AT",  # 就职于
    "supervises": "SUPERVISES",  # 监督
    "leads": "LEADS",  # 领导
    "founder": "FOUNDER",  # 创始人
    # 地理、位置相关
    "located in": "LOCATED_IN",  # 位于
    "institutional branches": "INSTITUTIONAL_BRANCHES",  # 机构分支
    # 政治外交相关
    "visited": "VISITED",  # 拜访
    "met": "MET",  # 会见
    "warned": "WARNED",  # 警告
    "cooperate with": "COOPERATES_WITH",  # 合作
    "competes with": "COMPETES_WITH",  # 竞争
    "hostile to": "HOSTILE_TO",  # 敌对
    "dispute": "DISPUTE",  # 争议
    "mediates": "MEDIATES",  # 调解
    "supports": "SUPPORTS",  # 支持
    "opposes": "OPPOSES",  # 反对
    "criticizes": "CRITICIZES",  # 批评
    # 企业与经济相关
    "communication": "COMMUNICATION",  # 商务沟通
    "shareholders": "SHAREHOLDERS",  # 个人股东
    "reliant on": "RELIANT_ON",  # 依赖
    "support": "PROVIDES_SUPPORT",  # 提供支持
    "invests in": "INVESTMENT",  # 投资
    "provides funding": "FUNDING",  # 资金提供
    "acquires company": "ACQUISITION",  # 收购
    # 事件相关
    "initiated": "INITIATED",  # 发起
    "discussed": "DISCUSSED",  # 讨论
    "influences": "INFLUENCES",  # 影响
    "participates in": "PARTICIPATES_IN",  # 参与
    "launches": "LAUNCHES",  # 启动
    "reported": "REPORTED",  # 报告
    "published": "PUBLISHED",  # 发布
    "criticized": "CRITICIZED",  # 批评
    "commented on": "COMMENTED_ON",  # 评论
    # 法律、冲突相关
    "accused": "ACCUSED",  # 被指控
    "denied": "DENIED",  # 否认
    "investigated": "INVESTIGATED",  # 被调查
    "filed lawsuit against": "FILED_LAWSUIT_AGAINST",  # 提起诉讼
    "imposed sanctions on": "IMPOSED_SANCTIONS_ON",  # 施加制裁
    "imposed tariffs on": "IMPOSED_TARIFFS_ON",  # 征收关税
        # 教育相关
    "teaches": "TEACHES",  # 教授
    "mentors": "MENTORS",  # 指导
    "provides training": "TRAINING",  # 培训
    # 国际关系相关
    "negotiates trade deal": "TRADE_NEGOTIATION",  # 贸易谈判
    "forms alliance with": "ALLIANCE",  # 结盟
    "mediates conflict": "MEDIATION",  # 调解
    "imposes sanctions": "SANCTIONS",  # 制裁
    # 其他
    "connected to": "CONNECTED_TO",  # 连接
    "associated with": "ASSOCIATED_WITH",  # 关联
    "partners with": "PARTNERS_WITH",  # 伙伴
    "shares information with": "SHARES_INFORMATION_WITH",  # 分享信息
    "facilitates": "FACILITATES",  # 促进
    "organizes": "ORGANIZES",  # 组织
}

relationship_type_mapping = list(xxx.values())
print(relationship_type_mapping)
# 连接 MySQL 数据库
def connect_to_database():
    try:
        return mysql.connector.connect(
            host='xxxx',
            user='xxxx',
            password='xxxx',
            database='xxxx'
        )
    except mysql.connector.Error as err:
        print(f"连接数据库时出错: {err}")
        return None

# 函数：获取指定作者的新闻数据
def get_tank_news(cursor):
    authors = ('Stimson Centre', 'National Endowment for Democracy')
    placeholders = ', '.join(['%s'] * len(authors))  # 根据作者数量生成占位符
    query = f"""
    SELECT link_hash, content, title, pubdate, title_cn, author
    FROM ustank_cn 
    WHERE author IN ({placeholders})
    """
    cursor.execute(query, authors)
    return cursor.fetchall()

def get_azure_openai_response(user_message: str, 
                               endpoint="base_url", 
                               api_key="your api key", 
                               model="gpt-4o", 
                               api_version="2023-03-15-preview") -> str:
    client = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version  
    )
    
    # 确保这些参数已经定义

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": user_message}],


        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"获取模型响应时出错: {e}")
        return None

import json
import ast
from datetime import datetime

def main():
    # 连接到 MySQL 数据库
    cnx = connect_to_database()
    if cnx is None:
        return

    try:
        with cnx.cursor() as cursor:
            # 获取指定作者的新闻数据
            news_items = get_tank_news(cursor)
            if not news_items:
                print("没有找到新闻数据。")
                return
            
            entity_file_path = "Filtered/entity1020.jsonl"
            relation_file_path = "Filtered/relation1020.jsonl"

            # 创建 JSON Lines 文件（如果文件不存在则创建）
            with open(entity_file_path, "a", encoding='utf-8') as entity_file, open(relation_file_path, "a", encoding='utf-8') as relation_file:
                for (link_hash, content, title, pubdate, title_cn, author) in news_items:
                    if not content:
                        print(f"跳过空内容的新闻 (链接哈希: {link_hash})")
                        continue

                    # 提取实体
                    prompt1 = f"""
                                You are an entity extraction tool that extracts entities related to think tanks, people, organizations, countries, and events from the news content. 
                                Requirements:
                                1. If no corresponding entities are extracted, the list should be empty, e.g., {{"tanks": [], "people": [], "organizations": [], "countries": [], "events": []}}.
                                2. The returned list should only contain the names of the five types of entities: think tanks, people, organizations, countries, and events, ensuring that no unrelated entities or content are included.
                                3. Do not return any non-formatted elements; return the format as: {{"tanks": ["tank1", "tank2"], "people": ["person1", "person2"], "organizations": ["organization1", "organization2"], "countries": ["country1", "country2"], "events": ["event1", "event2"]}}.
                                News content:
                                {content}
                                """


                    try:
                        response_name = get_azure_openai_response(user_message=prompt1)
                        print("-------------------------------------步骤一（实体提取）----------------------------------------")
                        print(f"正在处理新闻 (链接哈希: {link_hash}):\n{content}\n")
                        print(response_name)
                        
                        if response_name:
                            entities = ast.literal_eval(response_name[8:-4])
                            if isinstance(entities, dict):
                                print(f"大模型提取到的实体: {entities}")
                                entity_data = {
                                    "link_hash": link_hash,
                                    "extracted_entities": entities
                                }

                                # 写入实体数据到 JSON Lines 文件
                                json.dump(entity_data, entity_file, ensure_ascii=False)
                                entity_file.write('\n')  # 每条记录后换行

                                # 转换 pubdate 为字符串
                                if isinstance(pubdate, datetime):
                                    pubdate = pubdate.isoformat()


                                prompt2 = f"""
                                            你是一个知识图谱三元组提取工具，从新闻内容中提取指定实体之间的关系。
                                            输出要求：
                                            1. 仅提取以下实体之间的关系：{json.dumps(entities)}；
                                            2. 每个三元组格式为：实体1 - 关系 - 实体2，不得包含任何序号或多余文本；
                                            3. 关系名称需从给定关系类型中选择：{json.dumps(relationship_type_mapping)}，并确保准确、清晰；
                                            4. 每条三元组独占一行，仅输出三元组内容，不输出任何其他文本或符号；
                                            5. 格式示例：China - WARNED - United States。
                                            新闻内容：
                                            {content}
                                            """
                                print("*************************************步骤二（关系提取）**********************************")
                                print(prompt2)
                                response = get_azure_openai_response(user_message=prompt2)
                                print(response)

                                if response:
                                    # 使用换行符分割关系并去掉多余的空白
                                    relations = [relation.strip() for relation in response.split('\n') if relation.strip()]
                                    relation_data = {
                                        "extracted_relations": relations,
                                        "link_hash": link_hash,
                                        "title": title,
                                        "pubdate": pubdate,
                                        "author": author,
                                        "title_cn": title_cn,
                                        "type": ""
                                    }

                                    # 写入关系数据到 JSON Lines 文件
                                    json.dump(relation_data, relation_file, ensure_ascii=False)
                                    relation_file.write('\n')  # 每条记录后换行

                    except Exception as e:
                        print(f"在处理新闻 (链接哈希: {link_hash}) 时发生错误: {str(e)}")

    finally:
        cnx.close()

if __name__ == "__main__":
    main()
