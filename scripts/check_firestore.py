from google.cloud import firestore

PROJECT_ID = "quick-cache-484111-j4"
COLLECTION = "sensors"

db = firestore.Client(project=PROJECT_ID)

docs = list(db.collection(COLLECTION).limit(20).stream())
print(f"Found {len(docs)} docs in collection '{COLLECTION}':")

for d in docs:
    data = d.to_dict() or {}
    print("-", d.id, "| last_update:", data.get("last_update"))
