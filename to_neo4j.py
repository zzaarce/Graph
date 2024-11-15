from neo4j import GraphDatabase
import json

# 连接到 Neo4j 数据库
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("user", "password"))

def insert_entities(data):
    failed_count = 0
    with driver.session() as session:
        # 插入国家
        for country in data.get('extracted_entities', {}).get('countries', []):
            try:
                session.run("MERGE (c:Country {name: $name})", name=country)
            except Exception as e:
                print(f"Error inserting country '{country}': {e}")
                failed_count += 1

        # 插入组织
        for organization in data.get('extracted_entities', {}).get('organizations', []):
            try:
                session.run("MERGE (o:Organization {name: $name})", name=organization)
            except Exception as e:
                print(f"Error inserting organization '{organization}': {e}")
                failed_count += 1

        # 插入人物
        for person in data.get('extracted_entities', {}).get('people', []):
            try:
                session.run("MERGE (p:Person {name: $name})", name=person)
            except Exception as e:
                print(f"Error inserting person '{person}': {e}")
                failed_count += 1

        # 插入智库
        for tank in data.get('extracted_entities', {}).get('tanks', []):
            try:
                session.run("MERGE (t:Tank {name: $name})", name=tank)
            except Exception as e:
                print(f"Error inserting tank '{tank}': {e}")
                failed_count += 1

        # 插入事件
        for event in data.get('extracted_entities', {}).get('events', []):
            try:
                session.run("MERGE (e:Event {name: $name})", name=event)
            except Exception as e:
                print(f"Error inserting event '{event}': {e}")
                failed_count += 1

    return failed_count

def insert_relations(data):
    failed_count = 0
    with driver.session() as session:
        # 直接从数据中提取 subject、predicate 和 object 作为关系的三元组
        try:
            subject = data.get("subject")
            predicate = data.get("predicate")
            obj = data.get("object")

            # 如果任意关系组件缺失，跳过此数据
            if not subject or not predicate or not obj:
                print(f"Skipping relation with missing fields: {data}")
                return 0

            # 从其他字段获取附加属性
            relation_attributes = {
                "link_hash": data.get("link_hash", ""),
                "title": data.get("title", ""),
                "pubdate": data.get("pubdate", ""),
                "author": data.get("author", ""),
                "title_cn": data.get("title_cn", ""),
                "type": data.get("type", ""),
                "description": data.get("description", "")
            }

            # 使用 MERGE 创建关系并附加属性
            session.run(f"""
                MATCH (a {{name: $subject}}), (b {{name: $object}})
                MERGE (a)-[r:`{predicate}`]->(b)
                SET r += $attributes
            """, subject=subject, object=obj, predicate=predicate, attributes=relation_attributes)

        except Exception as e:
            print(f"Error inserting relation: {data} -> {e}")
            failed_count += 1

    return failed_count

# 读取 JSONL 文件并解析数据
entity_file_path = '/opt/USTank/graph/Filtered/entity1020.jsonl'
relation_file_path = '/opt/USTank/graph/Filtered/relation1020_processed_with_descriptions.jsonl'

total_failed = 0
try:
    with open(entity_file_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            total_failed += insert_entities(data)

    with open(relation_file_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            total_failed += insert_relations(data)
except Exception as e:
    print(f"Error reading file: {e}")

print(f"Total failed insertions: {total_failed}")
driver.close()
