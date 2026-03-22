from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from pymongo import MongoClient
import os
import json
import anthropic
from datetime import datetime
from bson import ObjectId


# =========================
# 1. LOAD ENVIRONMENT
# מה קורה כאן:
# - טוענים משתני סביבה מקובץ .env

load_dotenv()

api_key = os.getenv("ANTHROPIC_API_KEY")
mongo_uri = os.getenv("MONGODB_URI")

if not api_key:
    raise ValueError("Missing ANTHROPIC_API_KEY in .env file")

if not mongo_uri:
    raise ValueError("Missing MONGODB_URI in .env file")


# =========================
# 2. EXTERNAL CLIENTS
# מה קורה כאן:
# - חיבור ל-Claude
# - חיבור ל-Mongo
# - הגדרת collections

client = anthropic.Anthropic(api_key=api_key)

mongo_client = MongoClient(mongo_uri)
db = mongo_client["clinical_assistant"]

messages_collection = db["messages"]
principles_collection = db["principles"]
knowledge_collection = db["knowledge"]
protocols_collection = db["protocols"]
feedback_logs_collection = db["feedback_logs"]

# זיכרון זמני ל-Undo
# נשמר רק לזמן ריצה של השרת
last_saved_principle = None
last_saved_knowledge = None


# =========================
# 3. FASTAPI APP SETUP
# - יצירת אפליקציית FastAPI
# - הגדרת templates כדי לטעון index.html

app = FastAPI()
templates = Jinja2Templates(directory="templates")


# =========================
# 4. HELPER FUNCTIONS
# - כל פונקציות העזר של המערכת
# - טעינה ושמירה של messages / principles / knowledge
# - זיהוי intent
# - שליפת knowledge רלוונטי


# -------------------------
# load_chat(session_id)
# מחזיר את היסטוריית ההודעות של session מסוים בלבד
# כדי ששיחות שונות לא יתערבבו
# -------------------------
def load_chat(session_id):
    docs = messages_collection.find(
        {"session_id": session_id}
    ).sort("created_at", 1)

    return [
        {
            "role": doc["role"],
            "content": doc["content"]
        }
        for doc in docs
    ]


# -------------------------
# save_message(role, content, session_id)
# מה זה עושה:
# שומר הודעה חדשה ל-Mongo
# כדי לשייך כל הודעה לשיחה הנכונה
# -------------------------
def save_message(role, content, session_id, metadata=None):
    doc = {
        "role": role,
        "content": content,
        "session_id": session_id,
        "created_at": datetime.utcnow()
    }

    if metadata:
        doc["metadata"] = metadata

    result = messages_collection.insert_one(doc)
    return str(result.inserted_id)

def build_intent_classifier_prompt(user_message, recent_context=""):
    return f"""
You are an intent classifier for a medical assistant.

Your task is to classify the user's latest message into exactly one label.

Allowed labels:
- clinical_consult
- general_chat
- principle
- knowledge
- protocol

Definitions:

clinical_consult:
A clinical question, case, interpretation request, next-step decision, risk assessment, or follow-up on a patient scenario.

general_chat:
Greeting, thanks, casual chat, question about the tool, or non-clinical conversation.

principle:
A general instruction about how the assistant should think or respond.

knowledge:
A medical fact, rule, insight, or reusable clinical information that should be stored as knowledge.

protocol:
A department, hospital, local, or team-specific way of practicing medicine or making decisions.

Confidence:
- high
- medium
- low

Rules:
- Return exactly one label
- Return exactly one confidence value
- Do not answer the user
- Do not explain your reasoning
- Do not add extra text
- Output must be valid JSON only

Important routing policy:
- If the message is a patient case or clinical decision request, label it clinical_consult
- If uncertain between clinical_consult and a memory label, prefer clinical_consult
- Only use principle / knowledge / protocol when the user is clearly trying to teach or store something
- If uncertain between general_chat and clinical_consult, prefer clinical_consult
- For memory labels, use high confidence only when the intent is clearly explicit

Recent context:
{recent_context if recent_context else "None"}

Latest user message:
{user_message}

Return only JSON in this exact shape:
{{
  "label": "clinical_consult",
  "confidence": "high"
}}
"""    

def classify_message_intent(user_message, chat_history):
    recent_context_items = chat_history[-4:] if chat_history else []

    recent_context = "\n".join(
        [f"{item['role']}: {item['content']}" for item in recent_context_items]
    )

    prompt = build_intent_classifier_prompt(
        user_message=user_message,
        recent_context=recent_context
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=120,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )

        raw_text = response.content[0].text.strip()

        parsed = json.loads(raw_text)

        allowed_labels = {
            "clinical_consult",
            "general_chat",
            "principle",
            "knowledge",
            "protocol"
        }

        allowed_confidence = {"high", "medium", "low"}

        label = parsed.get("label")
        confidence = parsed.get("confidence")

        if label not in allowed_labels or confidence not in allowed_confidence:
            return {
                "label": "general_chat",
                "confidence": "low"
            }

        return {
            "label": label,
            "confidence": confidence
        }

    except Exception:
        return {
            "label": "general_chat",
            "confidence": "low"
        }

def format_response(text):
    sections = [
        "Most likely:",
        "Danger to rule out:",
        "What changes management now:",
        "Next step:"
    ]

    # מנקה רווחים מיותרים
    text = text.strip()

    # מוסיף ירידת שורה לפני כל section
    for section in sections:
        text = text.replace(section, f"\n{section}")

    # מסיר שורות ריקות מיותרות
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    text = "\n".join(lines)

    # אם חסר section, נוסיף אותו עם placeholder קצר
    for section in sections:
        if section not in text:
            text += f"\n\n{section}\nNot clearly stated."

    # ממיר ל-HTML קריא
    text = text.replace("\n\n", "<br><br>")
    text = text.replace("\n", "<br>")

    # מדגיש כותרות
    for section in sections:
        text = text.replace(section, f"<b>{section}</b>")

    return text.strip()  


# -------------------------
# load_principles()
# שולף את כל ה-principles שהמשתמש לימד את המערכת
# principles = איך לחשוב
# -------------------------
def load_principles():
    docs = principles_collection.find().sort("created_at", 1)
    return [doc["text"] for doc in docs]


# -------------------------
# save_principle(text)
# שומר principle חדש ב-Mongo
# -------------------------
def save_principle(text):
    principles_collection.insert_one({
        "text": text,
        "created_at": datetime.utcnow()
    })


# -------------------------
# delete_last_principle(text)
# מוחק principle לפי הטקסט שלו
# -------------------------
def delete_last_principle(text):
    principles_collection.delete_one({"text": text})


# -------------------------
# load_knowledge()
# שולף את כל פריטי ה-knowledge שנשמרו
# knowledge = מה לדעת
# -------------------------
def load_knowledge():
    docs = knowledge_collection.find().sort("created_at", 1)
    return [doc["text"] for doc in docs]

# -------------------------
# build_protocol_tags(text)
# בונה tags לפרוטוקולים.
# כרגע משתמש באותה לוגיקה של knowledge כדי להישאר עקביים ופשוטים.
# בהמשך אפשר להבדיל אם נרצה.
# -------------------------
def build_protocol_tags(text):
    return build_knowledge_tags(text)

# -------------------------
# load_protocols()
# שולף את כל ה-protocols שנשמרו
# protocols = איך עובדים אצלנו / גישת מחלקה / preference קליני מקומי
# -------------------------
def load_protocols():
    docs = protocols_collection.find().sort("created_at", 1)
    return [doc["text"] for doc in docs]


# -------------------------
# save_protocol(text)
# שומר protocol חדש ב-Mongo
# שומר גם type וגם tags כדי שנוכל בהמשך לשלוף רק protocols רלוונטיים
# -------------------------
def save_protocol(text):
    protocols_collection.insert_one({
        "text": text,
        "type": "protocol",
        "tags": build_protocol_tags(text),
        "weight": 5,
        "created_at": datetime.utcnow()
    })


# -------------------------
# delete_last_protocol(text)
# מוחק protocol לפי הטקסט שלו
# ישמש בהמשך גם ל-Undo
# -------------------------
def delete_last_protocol(text):
    protocols_collection.delete_one({"text": text})


# -------------------------
# get_relevant_protocols(user_message)
# שולף protocols רלוונטיים לפי tags של השאלה
# כרגע לוגיקה פשוטה:
# - מוציא tags מהשאלה
# - שולף protocols שיש להם overlap עם אותם tags
# - מחזיר עד 5 אחרונים
# -------------------------
def get_relevant_protocols(user_message):
    tags = extract_tags_from_query(user_message)

    if not tags:
        return []

    docs = list(protocols_collection.find({
        "tags": {"$in": tags}
    }))

    scored_docs = []

    for doc in docs:
        doc_tags = doc.get("tags", [])
        overlap = len(set(tags) & set(doc_tags))
        weight = doc.get("weight", 1)

        score = (overlap * 10) + weight
        scored_docs.append((score, doc))

    scored_docs.sort(key=lambda x: (x[0], x[1].get("created_at")), reverse=True)

    return [doc["text"] for score, doc in scored_docs[:5]]

# -------------------------
# build_knowledge_tags(text)
# מה זה עושה:
# מנסה לזהות מילות מפתח בסיסיות מתוך טקסט knowledge
# ושומר אותן כ-tags כדי שנוכל בעתיד לשלוף רק ידע רלוונטי
# כרגע זו גרסה פשוטה, rule-based
# -------------------------
import re

def build_knowledge_tags(text):
    text_lower = text.lower()
    tags = []

    tag_map = {
        "pregnancy": [
            "pregnancy", "pregnant", "ectopic", "iup", "pul",
            "fetal", "cardiac"
        ],
        "hcg": [
            "hcg", "β-hcg", "beta-hcg", "bhcg"
        ],
        "progesterone": [
            "progesterone"
        ],
        "tvus": [
            "tvus", "ultrasound", "scan", "nondiagnostic"
        ],
        "bleeding": [
            "bleeding", "spotting", "aub", "hemorrhage", "free fluid"
        ],
        "preeclampsia": [
            "preeclampsia", "pre-eclampsia", "magnesium", "severe features"
        ],
        "labor": [
            "labor", "labour", "induction", "ctg"
        ],
        "infection": [
            "infection", "pid", "fever", "sepsis", "chorioamnionitis"
        ],
        "dose": [
            "mg", "gram", "grams", "units", "iu"
        ],
        "guideline": [
            "guideline", "recommended", "first line", "second line",
            "indicated", "contraindicated", "prefer"
        ],
        "ectopic": [
            "ectopic", "adnexal mass"
        ],
        "methotrexate": [
            "methotrexate", "mtx"
        ]
    }

    for tag, keywords in tag_map.items():
        for keyword in keywords:
            if keyword in text_lower:
                tags.append(tag)
                break

    return tags

# -------------------------
# save_knowledge(text)
# מה זה עושה:
# שומר knowledge חדש ב-Mongo


def build_knowledge_tags(text):
    text_lower = text.lower()
    tags = []

    tag_map = {
        "pregnancy": [
            "pregnancy", "pregnant", "ectopic", "iup", "pul",
            "fetal", "cardiac"
        ],
        "hcg": [
            "hcg", "β-hcg", "beta-hcg", "bhcg"
        ],
        "progesterone": [
            "progesterone"
        ],
        "tvus": [
            "tvus", "ultrasound", "scan"
        ],
        "bleeding": [
            "bleeding", "spotting", "aub", "hemorrhage"
        ],
        "preeclampsia": [
            "preeclampsia", "pre-eclampsia", "magnesium", "severe features"
        ],
        "labor": [
            "labor", "labour", "induction", "ctg"
        ],
        "infection": [
            "infection", "pid", "fever", "sepsis", "chorioamnionitis"
        ],
        "dose": [
            "mg", "gram", "grams", "units", "iu"
        ],
        "guideline": [
            "guideline", "recommended", "first line", "second line",
            "indicated", "contraindicated"
        ]
    }

    for tag, keywords in tag_map.items():
        for keyword in keywords:
            if keyword in text_lower:
                tags.append(tag)
                break

    return tags

def save_knowledge(text):
    knowledge_collection.insert_one({
        "text": text,
        "type": "clinical",
        "tags": build_knowledge_tags(text),
        "weight": 3,
        "created_at": datetime.utcnow()
    })

def increase_knowledge_weight(text, amount=1):
    knowledge_collection.update_one(
        {"text": text},
        {"$inc": {"weight": amount}}
    )

def decrease_knowledge_weight(text, amount=1):
    knowledge_collection.update_one(
        {"text": text},
        {"$inc": {"weight": -amount}}
    )

def increase_protocol_weight(text, amount=1):
    protocols_collection.update_one(
        {"text": text},
        {"$inc": {"weight": amount}}
    )

def decrease_protocol_weight(text, amount=1):
    protocols_collection.update_one(
        {"text": text},
        {"$inc": {"weight": -amount}}
    )

# -------------------------
# delete_last_knowledge(text)
# מה זה עושה:
# מוחק knowledge item לפי הטקסט שלו
# משמש כרגע ל-Undo בסיסי
# -------------------------
def delete_last_knowledge(text):
    knowledge_collection.delete_one({"text": text})


# -------------------------
# detect_intent(message)
# מה זה עושה:
# מזהה אם ההודעה היא:
# - principle
# - knowledge
# - message רגיל
#
# principle = כלל חשיבה / heuristic / instruction
# knowledge = מידע מקצועי / guideline / עובדה
# message = כל השאר
#
# זה עדיין זיהוי פשוט, לא מושלם
# -------------------------
def detect_intent(message: str):
    """
    זיהוי בסיסי של סוג ההודעה:
    - principle = כלל חשיבה / heuristic / instruction
    - protocol = דרך עבודה מחלקתית / "אצלנו עושים ככה"
    - knowledge = מידע מקצועי / guideline / עובדה
    - message = כל השאר
    """
    msg = message.lower()

    principle_signals = [
        "always",
        "never",
        "avoid",
        "focus on",
        "remember",
        "separate",
        "prioritize",
        "do not"
    ]

    protocol_signals = [
        "in our department",
        "in our unit",
        "our protocol",
        "we prefer",
        "we usually",
        "we do not",
        "we avoid",
        "our practice",
        "at our hospital",
        "locally we"
        "in our hospital"
    ]

    knowledge_signals = [
        "is defined as",
        "is indicated",
        "is contraindicated",
        "means",
        "guideline",
        "important to remember",
        "usually",
        "typically",
        "threshold",
        "cutoff",
        "dose",
        "first line",
        "second line",
        "suggests",
        "associated with",
        "predicts",
        "indicates",
        "strongly suggests",
        "is linked to",
        "is consistent with"
    ]

    if any(signal in msg for signal in protocol_signals):
        return "protocol"

    if any(signal in msg for signal in principle_signals):
        return "principle"

    if any(signal in msg for signal in knowledge_signals):
        return "knowledge"

    return "message"


# -------------------------
# get_relevant_knowledge(user_message)
# מה זה עושה:
# כרגע מחזיר את 5 פריטי ה-knowledge האחרונים
# למה זה זמני:
# עוד לא בנינו retrieval חכם עם tags / relevance
# למה זה עדיין טוב:
# זה יציב, פשוט, ולא ישבור את המערכת
# -------------------------
def extract_tags_from_query(text):
    text_lower = text.lower()

    text_lower = text_lower.replace("β-hcg", "hcg")
    text_lower = text_lower.replace("beta-hcg", "hcg")
    text_lower = text_lower.replace("bhcg", "hcg")

    tags = []

    if any(word in text_lower for word in ["pregnancy", "pregnant", "ectopic", "iup", "pul"]):
        tags.append("pregnancy")

    if "hcg" in text_lower:
        tags.append("hcg")

    if "progesterone" in text_lower:
        tags.append("progesterone")

    if any(word in text_lower for word in ["tvus", "ultrasound", "scan"]):
        tags.append("tvus")

    if any(word in text_lower for word in ["bleeding", "spotting", "aub", "hemorrhage"]):
        tags.append("bleeding")

    if any(word in text_lower for word in ["preeclampsia", "magnesium"]) or "severe features" in text_lower:
        tags.append("preeclampsia")

    if any(word in text_lower for word in ["labor", "labour", "induction", "ctg"]):
        tags.append("labor")

    if any(word in text_lower for word in ["infection", "pid", "fever", "sepsis", "chorioamnionitis"]):
        tags.append("infection")

    return tags


def get_relevant_knowledge(user_message):
    tags = extract_tags_from_query(user_message)

    if not tags:
        return []

    docs = list(knowledge_collection.find({
        "tags": {"$in": tags}
    }))

    scored_docs = []

    for doc in docs:
        doc_tags = doc.get("tags", [])
        overlap = len(set(tags) & set(doc_tags))
        weight = doc.get("weight", 1)

        score = (overlap * 10) + weight
        scored_docs.append((score, doc))

    scored_docs.sort(key=lambda x: (x[0], x[1].get("created_at")), reverse=True)

    return [doc["text"] for score, doc in scored_docs[:5]]

def save_feedback_log(message_id, direction, used_knowledge, used_protocols):
    feedback_logs_collection.insert_one({
        "message_id": message_id,
        "direction": direction,
        "used_knowledge": used_knowledge,
        "used_protocols": used_protocols,
        "created_at": datetime.utcnow()
    })

# =========================
# 5. HOME PAGE ROUTE
# מה קורה כאן:
# טוען את index.html מתוך templates
# הסקשן נגמר לפני /message
# =========================
@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# =========================
# 6. CHAT MESSAGE ROUTE
# מה קורה כאן:
# - מקבל הודעה מה-frontend
# - מזהה intent
# - אם זה principle / knowledge -> שומר אוטומטית
# - אם לא -> שולח ל-Claude
# - שומר את השיחה ב-Mongo
# הסקשן נגמר לפני /undo
# =========================
@app.post("/message")
async def handle_message(request: Request):
    global last_saved_principle
    global last_saved_knowledge
    global last_saved_protocol

    try:
        data = await request.json()
        user_message = data.get("message", "").strip()
        session_id = data.get("session_id")

        if not user_message:
            return JSONResponse({"reply": "Empty message."})

        if not session_id:
            return JSONResponse({"reply": "Missing session_id."})

        chat_history = load_chat(session_id)
        principles = load_principles()
        knowledge_items = load_knowledge()
        protocol_items = load_protocols()

        knowledge = get_relevant_knowledge(user_message)
        protocols = get_relevant_protocols(user_message)

        classifier_result = classify_message_intent(user_message, chat_history)
        intent = classifier_result["label"]
        confidence = classifier_result["confidence"]

        print("DEBUG CLASSIFIER LABEL:", intent)
        print("DEBUG CLASSIFIER CONFIDENCE:", confidence)

        undo_flag = False

        # -------------------------
        # 1. MEMORY FLOWS
        # נשמור רק אם confidence גבוה
        # -------------------------

        if intent == "protocol" and confidence == "high":
            existing_protocols_lower = [p.lower() for p in protocol_items]

            if user_message.lower() not in existing_protocols_lower:
                save_protocol(user_message)
                last_saved_protocol = user_message
                undo_flag = True
                reply = "Saved as a protocol. Will use this as department-level guidance."
            else:
                reply = "Already saved as a protocol."

            save_message("user", user_message, session_id)
            save_message("assistant", reply, session_id)

            return JSONResponse({
                "reply": reply,
                "undo": undo_flag,
                "undo_type": "protocol" if undo_flag else None,
                "show_feedback": False,
                "assistant_message_id": None
            })

        if intent == "principle" and confidence == "high":
            existing_principles_lower = [p.lower() for p in principles]

            if user_message.lower() not in existing_principles_lower:
                save_principle(user_message)
                last_saved_principle = user_message
                undo_flag = True
                reply = "Saved as a principle. Will apply this going forward."
            else:
                reply = "Already saved. Still applying it."

            save_message("user", user_message, session_id)
            save_message("assistant", reply, session_id)

            return JSONResponse({
                "reply": reply,
                "undo": undo_flag,
                "undo_type": "principle" if undo_flag else None,
                "show_feedback": False,
                "assistant_message_id": None
            })

        if intent == "knowledge" and confidence == "high":
            existing_knowledge_lower = [k.lower() for k in knowledge_items]

            if user_message.lower() not in existing_knowledge_lower:
                save_knowledge(user_message)
                last_saved_knowledge = user_message
                undo_flag = True
                reply = "Saved as knowledge for future use."
            else:
                reply = "Already saved in knowledge."

            save_message("user", user_message, session_id)
            save_message("assistant", reply, session_id)

            return JSONResponse({
                "reply": reply,
                "undo": undo_flag,
                "undo_type": "knowledge" if undo_flag else None,
                "show_feedback": False,
                "assistant_message_id": None
            })

        # -------------------------
        # 2. ROUTING POLICY
        # אם זה לא memory-save, נחליט בין clinical / general
        # -------------------------

        # אם המודל לא בטוח ב-memory label, לא נשמור.
        # במקום זה נעביר ל-clinical או general.
        if intent in ["principle", "knowledge", "protocol"] and confidence != "high":
            intent = "general_chat"

        messages = []
        for item in chat_history[-6:]:
            messages.append({
                "role": item["role"],
                "content": item["content"]
            })

        messages.append({
            "role": "user",
            "content": user_message
        })

        principles_text = "\n".join(f"- {p}" for p in principles)
        knowledge_text = "\n".join(f"- {k}" for k in knowledge)
        protocols_text = "\n".join(f"- {p}" for p in protocols)

        # -------------------------
        # 3. CLINICAL MODE
        # -------------------------
        if intent == "clinical_consult":
            system_prompt = f"""
You are a senior OB-GYN consultant.

Think like a real clinical decision maker, not a textbook.

Context:

Saved user principles:
{principles_text if principles_text else "- No saved principles yet"}

Relevant clinical knowledge:
{knowledge_text if knowledge_text else "- No relevant knowledge"}

Department protocols:
{protocols_text if protocols_text else "- No relevant protocols"}

Core behavior:
- Be sharp
- Be concise
- Focus only on what changes management now
- Ignore anything that does not affect decisions
- Do not explain basics

Decision hierarchy:
1. If a relevant protocol exists → follow it
2. If no protocol → use clinical knowledge
3. If evidence is unclear → reason clinically

Protocols override general knowledge.

Clinical priority:
1. Is this unstable or time-sensitive?
2. What must be ruled out immediately?
3. What is most likely?

Clinical thinking:
Always separate:
- Most likely
- Dangerous to rule out

If data is missing:
- Say exactly what is missing
- Say why it matters

If multiple paths exist:
- Acknowledge briefly
- Choose the safest path

Challenge wrong assumptions.

Uncertainty handling:
- Do not assume certainty
- If unclear → say it
- Use:
  "uncertain"
  "depends on"
  "no clear consensus"
- If unsure:
  define what the decision depends on
  state safest working assumption
- If confidence is low:
  say what would increase confidence
- Do NOT guess

Interaction discipline:
- Ask at most ONE question
- Only if it changes management now
- If not critical → do not ask
- Do not repeat questions
- Do not say "already answered"
- Always respond as a fresh case

Output discipline:
- No explanations of reasoning
- No thinking out loud
- No long text
- No full differentials
- Prefer one clear path
- Avoid vague language
- Avoid "could be" unless necessary

Use of stored data:
- Use protocols and knowledge naturally
- Do NOT mention them
- They should feel like experience

Formatting (critical):
- Each section MUST start on a new line
- Add a blank line between sections
- Each idea on its own line
- No dense paragraphs
- No inline formatting (**)

Output format (strict):

Most likely:
<1-2 short lines>

Danger to rule out:
<1-2 short lines>

What changes management now:
<1-3 short lines>

Next step:
<1-2 short lines>

Quality bar:
- If obvious → be shorter
- If uncertain → define exactly why
- If protocol applies → be decisive
- Do not hedge unnecessarily
"""
        else:
            # -------------------------
            # 4. GENERAL MODE
            # -------------------------
            system_prompt = """
You are a concise, natural, professional assistant.

Behavior:
- Reply briefly and naturally
- Do not use clinical sections
- Do not force medical structure
- For greetings or casual messages, respond like a human expert assistant
- Do not sound robotic
- Keep the reply short
"""

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            system=system_prompt,
            messages=messages
        )

        reply = response.content[0].text

        if intent == "clinical_consult":
            reply = format_response(reply)

        save_message("user", user_message, session_id)

        assistant_message_id = save_message(
            "assistant",
            reply,
            session_id,
            metadata={
                "used_knowledge": knowledge,
                "used_protocols": protocols,
                "intent": intent,
                "confidence": confidence
            }
        )

        show_feedback = intent == "clinical_consult" and len(reply) > 80

        return JSONResponse({
            "reply": reply,
            "undo": False,
            "undo_type": None,
            "show_feedback": show_feedback,
            "assistant_message_id": assistant_message_id
        })

    except Exception as e:
        return JSONResponse({"reply": f"ERROR: {str(e)}"})

# =========================
# 7. UNDO ROUTE
# מה קורה כאן:
# מאפשר לבטל את השמירה האחרונה
# כרגע עובד על principle / knowledge אחרונים בלבד
# הסקשן נגמר בסוף הקובץ
# =========================
@app.post("/undo")
def undo():
    global last_saved_principle
    global last_saved_knowledge
    global last_saved_protocol

    try:
        if last_saved_protocol:
            delete_last_protocol(last_saved_protocol)
            last_saved_protocol = None
            return {"status": "undone protocol"}

        if last_saved_knowledge:
            delete_last_knowledge(last_saved_knowledge)
            last_saved_knowledge = None
            return {"status": "undone knowledge"}

        if last_saved_principle:
            delete_last_principle(last_saved_principle)
            last_saved_principle = None
            return {"status": "undone principle"}

        return {"status": "nothing to undo"}

    except Exception as e:
        return {"status": f"error: {str(e)}"}

@app.post("/feedback")
async def feedback(request: Request):
    try:
        data = await request.json()
        message_id = data.get("message_id")
        direction = data.get("direction")

        if not message_id or direction not in ["up", "down"]:
            return JSONResponse({"status": "invalid request"})

        message_doc = messages_collection.find_one({"_id": ObjectId(message_id)})

        if not message_doc:
            return JSONResponse({"status": "message not found"})

        metadata = message_doc.get("metadata", {})
        used_knowledge = metadata.get("used_knowledge", [])
        used_protocols = metadata.get("used_protocols", [])

        # שומרים log של הפידבק
        save_feedback_log(
            message_id=message_id,
            direction=direction,
            used_knowledge=used_knowledge,
            used_protocols=used_protocols
        )

        if direction == "up":
            for item in used_knowledge:
                increase_knowledge_weight(item, 1)
            for item in used_protocols:
                increase_protocol_weight(item, 1)
            return JSONResponse({"status": "marked useful"})

        if direction == "down":
            for item in used_knowledge:
                decrease_knowledge_weight(item, 1)
            for item in used_protocols:
                decrease_protocol_weight(item, 1)
            return JSONResponse({"status": "marked off"})

        return JSONResponse({"status": "no action"})

    except Exception as e:
        return JSONResponse({"status": f"error: {str(e)}"})