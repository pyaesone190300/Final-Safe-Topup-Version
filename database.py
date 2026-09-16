import os
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')

if not MONGO_URI:
    print("❌ Error: .env ဖိုင်ထဲတွင် MONGO_URI မပါဝင်ပါ။")
    exit()

try:
    client = AsyncIOMotorClient(
        MONGO_URI, 
        serverSelectionTimeoutMS=5000, 
        maxPoolSize=50
    )
    db = client['smile_vwallet_db']
    
    resellers_col = db['resellers']
    settings_col = db['settings']
    orders_col = db['orders']
    topups_col = db['topups']
    
    print("✅ Async MongoDB (Motor) ချိတ်ဆက်မှု အောင်မြင်ပါသည်။")
    
except Exception as e:
    print(f"❌ MongoDB ချိတ်ဆက်မှု မအောင်မြင်ပါ: {e}")
    exit()

MMT = datetime.timezone(datetime.timedelta(hours=6, minutes=30))


async def setup_indexes():
    try:
        await resellers_col.create_index("tg_id", unique=True)
        await orders_col.create_index([("tg_id", 1), ("timestamp", -1)])
        await topups_col.create_index([("tg_id", 1), ("created_at", -1)])
        await topups_col.create_index("activation_code", unique=True)
    except Exception as e:
        print(f"⚠️ Index ဖန်တီးရာတွင် အမှားရှိပါသည်: {e}")


async def init_owner(owner_id):
    owner_str = str(owner_id)
    existing_owner = await resellers_col.find_one({"tg_id": owner_str})
    
    if not existing_owner:
        await resellers_col.insert_one({
            "tg_id": owner_str,
            "username": "Owner"
        })


async def get_main_cookie():
    doc = await settings_col.find_one({"type": "main_cookie"})
    
    if doc:
        return doc.get("cookie", "")
    return ""


async def update_main_cookie(cookie_str):
    await settings_col.update_one(
        {"type": "main_cookie"},
        {"$set": {"cookie": cookie_str}},
        upsert=True
    )


async def get_reseller(tg_id):
    return await resellers_col.find_one({"tg_id": str(tg_id)})


async def get_all_resellers():
    cursor = resellers_col.find({})
    return await cursor.to_list(length=None)


async def add_reseller(tg_id, username):
    tg_id_str = str(tg_id)
    existing_user = await resellers_col.find_one({"tg_id": tg_id_str})
    
    if not existing_user:
        await resellers_col.insert_one({
            "tg_id": tg_id_str,
            "username": username
        })
        return True
        
    return False


async def remove_reseller(tg_id):
    result = await resellers_col.delete_one({"tg_id": str(tg_id)})
    return result.deleted_count > 0


async def save_order(tg_id, game_id, zone_id, item_name, price, order_id, status="success"):
    now = datetime.datetime.now(MMT)
    
    order_data = {
        "tg_id": str(tg_id),
        "game_id": str(game_id),
        "zone_id": str(zone_id),
        "item_name": item_name,
        "price": round(float(price), 2),
        "order_id": str(order_id),
        "status": status,
        "date_str": now.strftime("%I:%M:%S %p %d.%m.%Y"), 
        "timestamp": now 
    }
    await orders_col.insert_one(order_data)


async def get_user_history(tg_id, limit=50):
    cursor = orders_col.find(
        {"tg_id": str(tg_id)}, 
        {"_id": 0} 
    ).sort("timestamp", -1).limit(limit)
    
    return await cursor.to_list(length=limit)


async def clear_user_history(tg_id):
    result = await orders_col.delete_many({"tg_id": str(tg_id)})
    return result.deleted_count


async def set_vip_status(tg_id, is_vip: bool):
    result = await resellers_col.update_one(
        {"tg_id": str(tg_id)},
        {"$set": {"is_vip": is_vip}}
    )
    return result.modified_count > 0


async def get_top_customers(limit=10):
    pipeline = [
        {"$match": {"status": "success"}},
        {"$group": {
            "_id": "$tg_id",
            "total_spent": {"$sum": "$price"},
            "order_count": {"$sum": 1}
        }},
        {"$sort": {"total_spent": -1}},
        {"$limit": limit}
    ]
    cursor = orders_col.aggregate(pipeline)
    return await cursor.to_list(length=limit)


async def get_today_orders_summary():
    now = datetime.datetime.now(MMT)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    pipeline = [
        {"$match": {"status": "success", "timestamp": {"$gte": start_of_day}}},
        {"$group": {
            "_id": None,
            "total_spent": {"$sum": "$price"},
            "total_orders": {"$sum": 1}
        }}
    ]
    cursor = orders_col.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if result:
        return result[0]
    return {"total_spent": 0.0, "total_orders": 0}


async def add_scammer(game_id: str):
    await db.scammers.update_one(
        {"game_id": game_id}, 
        {"$set": {"game_id": game_id}}, 
        upsert=True
    )
    return True


async def remove_scammer(game_id: str):
    result = await db.scammers.delete_one({"game_id": game_id})
    return result.deleted_count > 0


async def get_all_scammers():
    cursor = db.scammers.find({})
    return [doc["game_id"] async for doc in cursor]



async def create_topup_record(tg_id, activation_code, region):
    from pymongo.errors import DuplicateKeyError
    now = datetime.datetime.now(datetime.timezone.utc)
    doc = {
        "tg_id": str(tg_id),
        "activation_code": str(activation_code),
        "region": str(region),
        "status": "queued",
        "created_at": now,
        "updated_at": now,
    }
    try:
        await topups_col.insert_one(doc)
        return True, doc
    except DuplicateKeyError:
        existing = await topups_col.find_one({"activation_code": str(activation_code)})
        return False, existing

async def update_topup_status(activation_code, status, **extra):
    fields = {"status": status, "updated_at": datetime.datetime.now(datetime.timezone.utc), **extra}
    return await topups_col.update_one(
        {"activation_code": str(activation_code)}, {"$set": fields}
    )
