from datetime import datetime

from db import knowledge_collection, principles_collection, protocols_collection


def load_principles():
    docs = principles_collection.find().sort("created_at", 1)
    return [doc["text"] for doc in docs]


def save_principle(text):
    principles_collection.insert_one({"text": text, "created_at": datetime.utcnow()})


def delete_last_principle(text):
    principles_collection.delete_one({"text": text})


def principle_exists(text, principles=None):
    source = principles if principles is not None else load_principles()
    return text.lower() in {item.lower() for item in source}


def load_knowledge():
    docs = knowledge_collection.find().sort("created_at", 1)
    return [doc["text"] for doc in docs]


def build_knowledge_tags(text):
    text_lower = text.lower()
    tags = []
    tag_map = {
        "pregnancy": ["pregnancy", "pregnant", "ectopic", "iup", "pul", "fetal", "cardiac"],
        "hcg": ["hcg", "beta-hcg", "bhcg", "β-hcg"],
        "progesterone": ["progesterone"],
        "tvus": ["tvus", "ultrasound", "scan", "nondiagnostic"],
        "bleeding": ["bleeding", "spotting", "aub", "hemorrhage", "free fluid"],
        "preeclampsia": ["preeclampsia", "pre-eclampsia", "magnesium", "severe features"],
        "labor": ["labor", "labour", "induction", "ctg"],
        "infection": ["infection", "pid", "fever", "sepsis", "chorioamnionitis"],
        "dose": ["mg", "gram", "grams", "units", "iu"],
        "guideline": [
            "guideline",
            "recommended",
            "first line",
            "second line",
            "indicated",
            "contraindicated",
            "prefer",
        ],
        "ectopic": ["ectopic", "adnexal mass"],
        "methotrexate": ["methotrexate", "mtx"],
    }

    for tag, keywords in tag_map.items():
        if any(keyword in text_lower for keyword in keywords):
            tags.append(tag)

    return tags


def build_protocol_tags(text):
    return build_knowledge_tags(text)


def save_knowledge(text):
    knowledge_collection.insert_one(
        {
            "text": text,
            "type": "clinical",
            "tags": build_knowledge_tags(text),
            "weight": 3,
            "created_at": datetime.utcnow(),
        }
    )


def save_protocol(text):
    protocols_collection.insert_one(
        {
            "text": text,
            "type": "protocol",
            "tags": build_protocol_tags(text),
            "weight": 5,
            "created_at": datetime.utcnow(),
        }
    )


def load_protocols():
    docs = protocols_collection.find().sort("created_at", 1)
    return [doc["text"] for doc in docs]


def delete_last_knowledge(text):
    knowledge_collection.delete_one({"text": text})


def delete_last_protocol(text):
    protocols_collection.delete_one({"text": text})


def knowledge_exists(text, knowledge_items=None):
    source = knowledge_items if knowledge_items is not None else load_knowledge()
    return text.lower() in {item.lower() for item in source}


def protocol_exists(text, protocol_items=None):
    source = protocol_items if protocol_items is not None else load_protocols()
    return text.lower() in {item.lower() for item in source}


def extract_tags_from_query(text):
    text_lower = text.lower()
    replacements = {"β-hcg": "hcg", "beta-hcg": "hcg", "bhcg": "hcg"}
    for old, new in replacements.items():
        text_lower = text_lower.replace(old, new)

    tags = []

    if any(word in text_lower for word in ["pregnancy", "pregnant", "ectopic", "iup", "pul"]):
        tags.append("pregnancy")
    if "hcg" in text_lower:
        tags.append("hcg")
    if "progesterone" in text_lower:
        tags.append("progesterone")
    if any(word in text_lower for word in ["tvus", "ultrasound", "scan", "nondiagnostic"]):
        tags.append("tvus")
    if any(word in text_lower for word in ["bleeding", "spotting", "aub", "hemorrhage", "free fluid"]):
        tags.append("bleeding")
    if any(word in text_lower for word in ["preeclampsia", "pre-eclampsia", "magnesium"]) or "severe features" in text_lower:
        tags.append("preeclampsia")
    if any(word in text_lower for word in ["labor", "labour", "induction", "ctg"]):
        tags.append("labor")
    if any(word in text_lower for word in ["infection", "pid", "fever", "sepsis", "chorioamnionitis"]):
        tags.append("infection")
    if any(word in text_lower for word in ["methotrexate", "mtx"]):
        tags.append("methotrexate")

    return tags


def _score_retrieved_docs(docs, query_tags):
    scored_docs = []
    for doc in docs:
        doc_tags = doc.get("tags", [])
        overlap = len(set(query_tags) & set(doc_tags))
        weight = doc.get("weight", 1)
        score = (overlap * 10) + weight
        scored_docs.append((score, doc))

    scored_docs.sort(key=lambda item: (item[0], item[1].get("created_at")), reverse=True)
    return [doc["text"] for score, doc in scored_docs[:5]]


def get_relevant_knowledge(user_message):
    tags = extract_tags_from_query(user_message)
    if not tags:
        return []

    docs = list(knowledge_collection.find({"tags": {"$in": tags}}))
    return _score_retrieved_docs(docs, tags)


def get_relevant_protocols(user_message):
    tags = extract_tags_from_query(user_message)
    if not tags:
        return []

    docs = list(protocols_collection.find({"tags": {"$in": tags}}))
    return _score_retrieved_docs(docs, tags)


def increase_knowledge_weight(text, amount=1):
    knowledge_collection.update_one({"text": text}, {"$inc": {"weight": amount}})


def decrease_knowledge_weight(text, amount=1):
    knowledge_collection.update_one({"text": text}, {"$inc": {"weight": -amount}})


def increase_protocol_weight(text, amount=1):
    protocols_collection.update_one({"text": text}, {"$inc": {"weight": amount}})


def decrease_protocol_weight(text, amount=1):
    protocols_collection.update_one({"text": text}, {"$inc": {"weight": -amount}})
