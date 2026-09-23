import os
import time
import datetime
import random
import re
import asyncio
from dataclasses import dataclass
import html
import json
import concurrent.futures
from collections import defaultdict
from dotenv import load_dotenv

from bs4 import BeautifulSoup
from DrissionPage import ChromiumPage, ChromiumOptions
from curl_cffi.requests import AsyncSession

from aiogram import Bot, Dispatcher, BaseMiddleware, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command, or_f
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

import database as db

# ==========================================
# 1. Configuration & Global Variables
# ==========================================
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID', 1318826936))
GOOGLE_EMAIL = os.getenv('GOOGLE_EMAIL')
GOOGLE_PASS = os.getenv('GOOGLE_PASS')

if not BOT_TOKEN:
    print("❌ Error: BOT_TOKEN is missing in the .env file.")
    exit()

MMT = datetime.timezone(datetime.timedelta(hours=6, minutes=30))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

IS_MAINTENANCE = False
GLOBAL_SCAMMERS = set()
user_locks = defaultdict(asyncio.Lock)

# Shared account/session transaction lane.
smile_account_lock = asyncio.Lock()
TOPUP_QUEUE = asyncio.Queue()
TOPUP_WORKER_TASK = None

@dataclass
class TopupJob:
    user_id: str
    activation_code: str
    region: str
    loading_message: object

api_semaphore = asyncio.Semaphore(10)
auth_lock = asyncio.Lock()

BR_EMOJI = "5228878788867142213"   
PH_EMOJI = "5231361434583049965"

WEBSHARE_PROXIES = [p.strip() for p in os.getenv("WEBSHARE_PROXIES", "").split(",") if p.strip()]
last_login_time = 0
GLOBAL_SCRAPER = None
GLOBAL_COOKIE_STR = ""
GLOBAL_CSRF = {'mlbb_br': None, 'mlbb_ph': None, 'mcc_br': None, 'mcc_ph': None}

# ==========================================
# 2. Package Definitions
# ==========================================
DOUBLE_DIAMOND_PACKAGES = {
    'b50': [{'pid': '22590', 'price': 39.0, 'name': '50+50 💎'}],
    'b150': [{'pid': '22591', 'price': 116.9, 'name': '150+150 💎'}],
    'b250': [{'pid': '22592', 'price': 187.5, 'name': '250+250 💎'}],
    'b500': [{'pid': '22593', 'price': 385.0, 'name': '500+500 💎'}],
}

BR_PACKAGES = {
    '86': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}],
    '172': [{'pid': '23', 'price': 122.0, 'name': '172 💎'}],
    '257': [{'pid': '25', 'price': 177.5, 'name': '257 💎'}],
    '343': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}],
    '429': [{'pid': '23', 'price': 122.0, 'name': '172 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}],
    '514': [{'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}],
    '600': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}],
    '706': [{'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '878': [{'pid': '23', 'price': 122.0, 'name': '172 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '963': [{'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '1049': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '1135': [{'pid': '23', 'price': 122.0, 'name': '172 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '1412': [{'pid': '26', 'price': 480.0, 'name': '706 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '1584': [{'pid': '23', 'price': 122.0, 'name': '172 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '1755': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '2195': [{'pid': '27', 'price': 1453.0, 'name': '2195 💎'}],
    '2538': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '27', 'price': 1453.0, 'name': '2195 💎'}],
    '2901': [{'pid': '27', 'price': 1453.0, 'name': '2195 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}],
    '3244': [{'pid': '13', 'price': 61.5, 'name': '86 💎'}, {'pid': '25', 'price': 177.5, 'name': '257 💎'}, {'pid': '26', 'price': 480.0, 'name': '706 💎'}, {'pid': '27', 'price': 1453.0, 'name': '2195 💎'}],
    '3688': [{'pid': '28', 'price': 2424.0, 'name': '3688 💎'}],
    '5532': [{'pid': '29', 'price': 3660.0, 'name': '5532 💎'}],
    '9288': [{'pid': '30', 'price': 6079.0, 'name': '9288 💎'}],
    'meb': [{'pid': '26556', 'price': 196.5, 'name': 'Epic Monthly Package'}],
    'tp': [{'pid': '33', 'price': 402.5, 'name': 'Twilight Passage'}],
    'web': [{'pid': '26555', 'price': 39.0, 'name': 'Elite Weekly Paackage'}],
    'wp': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'}],
    'wp1': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'}],
    'wp2': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(2)],
    'wp3': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(3)],
    'wp4': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(4)],
    'wp5': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(5)],
    'wp6': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(6)],
    'wp7': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(7)],
    'wp8': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(8)],
    'wp9': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(9)],
    'wp10': [{'pid': '16642', 'price': 76.0, 'name': 'Weekly Pass'} for _ in range(10)]
}

PH_PACKAGES = {
    '11': [{'pid': '212', 'price': 9.50, 'name': '11 💎'}],
    '22': [{'pid': '213', 'price': 19.00, 'name': '22 💎'}],
    '33': [{'pid': '213', 'price': 19.00, 'name': '22 💎'}, {'pid': '212', 'price': 9.50, 'name': '11 💎'}],
    '44': [{'pid': '213', 'price': 19.00, 'name': '22 💎'}, {'pid': '213', 'price': 19.00, 'name': '22 💎'}],
    '56': [{'pid': '214', 'price': 47.50, 'name': '56 💎'}],
    '112': [{'pid': '215', 'price': 95.00, 'name': '112 💎'}],
    '223': [{'pid': '216', 'price': 190.00, 'name': '223 💎'}],
    '336': [{'pid': '217', 'price': 285.00, 'name': '336 💎'}],
    '570': [{'pid': '218', 'price': 475.00, 'name': '570 💎'}],
    '1163': [{'pid': '219', 'price': 950.00, 'name': '1163 💎'}],
    '2398': [{'pid': '220', 'price': 1900.00, 'name': '2398 💎'}],
    '6042': [{'pid': '221', 'price': 4750.00, 'name': '6042 💎'}],
    'tp': [{'pid': '214', 'price': 475.00, 'name': 'twilight pass 💎'}],
    'wp': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'}],
    'wp1': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'}],
    'wp2': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(2)],
    'wp3': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(3)],
    'wp4': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(4)],
    'wp5': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(5)],
    'wp6': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(6)],
    'wp7': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(7)],
    'wp8': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(8)],
    'wp9': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(9)],
    'wp10': [{'pid': '16641', 'price': 95.00, 'name': 'Weekly Pass'} for _ in range(10)]
}

MCC_PACKAGES = {
    '86': [{'pid': '23825', 'price': 62.5, 'name': '86 💎'}],
    '172': [{'pid': '23826', 'price': 125.0, 'name': '172 💎'}],
    '257': [{'pid': '23827', 'price': 187.0, 'name': '257 💎'}],
    '343': [{'pid': '23828', 'price': 250.0, 'name': '343 💎'}],
    '429': [{'pid': '23826', 'price': 122.0, 'name': '172 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}],
    '516': [{'pid': '23829', 'price': 375.0, 'name': '516 💎'}],
    '600': [{'pid': '23825', 'price': 62.5, 'name': '86 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23827', 'price': 177.5, 'name': '257 💎'}],
    '706': [{'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '878': [{'pid': '23826', 'price': 125.0, 'name': '172 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '963': [{'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '1049': [{'pid': '23825', 'price': 62.5, 'name': '86 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '1135': [{'pid': '23826', 'price': 125.0, 'name': '172 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '1346': [{'pid': '23831', 'price': 937.5, 'name': '1346 💎'}],
    '1412': [{'pid': '23830', 'price': 500.0, 'name': '706 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '1584': [{'pid': '23826', 'price': 125.0, 'name': '172 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}, {'pid': '23830', 'price': 480.0, 'name': '706 💎'}],
    '1755': [{'pid': '23825', 'price': 62.5, 'name': '86 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '1825': [{'pid': '23832', 'price': 1250.0, 'name': '1825 💎'}],
    '2195': [{'pid': '23833', 'price': 1500.0, 'name': '2195 💎'}],
    '2538': [{'pid': '23825', 'price': 62.5, 'name': '86 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23833', 'price': 1500.0, 'name': '2195 💎'}],
    '2901': [{'pid': '23833', 'price': 1500.0, 'name': '2195 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}],
    '3244': [{'pid': '23825', 'price': 62.5, 'name': '86 💎'}, {'pid': '23827', 'price': 187.0, 'name': '257 💎'}, {'pid': '23830', 'price': 500.0, 'name': '706 💎'}, {'pid': '23833', 'price': 1500.0, 'name': '2195 💎'}],
    '3688': [{'pid': '23834', 'price': 2500.0, 'name': '3688 💎'}],
    '5532': [{'pid': '23835', 'price': 3750.0, 'name': '5532 💎'}],
    '9288': [{'pid': '23836', 'price': 6250.0, 'name': '9288 💎'}],
    'b150': [{'pid': '23838', 'price': 120.0, 'name': '150+150 💎'}],
    'b250': [{'pid': '23839', 'price': 200.0, 'name': '250+250 💎'}],
    'b50': [{'pid': '23837', 'price': 40.0, 'name': '50+50 💎'}],
    'b500': [{'pid': '23840', 'price': 400.0, 'name': '500+500 💎'}],
    'wp': [{'pid': '23841', 'price': 99.90, 'name': 'Weekly Pass'}],
}

PH_MCC_PACKAGES = {
    '5': [{'pid': '23906', 'price': 4.75, 'name': '5 💎'}],
    '11': [{'pid': '23907', 'price': 9.03, 'name': '11 💎'}],
    '22': [{'pid': '23908', 'price': 18.05, 'name': '22 💎'}],
    '56': [{'pid': '23909', 'price': 45.13, 'name': '56 💎'}],
    '112': [{'pid': '23910', 'price': 90.25, 'name': '112 💎'}],
    '223': [{'pid': '23911', 'price': 180.50, 'name': '223 💎'}],
    '339': [{'pid': '23912', 'price': 270.75, 'name': '339 💎'}],
    '570': [{'pid': '23913', 'price': 451.25, 'name': '578 💎'}],
    '1163': [{'pid': '23914', 'price': 902.50, 'name': '1163 💎'}],
    '2398': [{'pid': '23915', 'price': 1805.00, 'name': '2398 💎'}],
    '6042': [{'pid': '23916', 'price': 4512.50, 'name': '6042 💎'}],
    'wp': [{'pid': '23922', 'price': 95.00, 'name': 'wp 💎'}],
    'lukas': [{'pid': '25600', 'price': 47.45, 'name': 'lukas battle bounty💎'}],
    'battlefordiscounts': [{'pid': '25601', 'price': 47.45, 'name': 'battlefordiscounts 💎'}],
}

# ==========================================
# 3. Helpers Functions
# ==========================================
async def is_authorized(user_id: int):
    if user_id == OWNER_ID:
        return True
        
    user = await db.get_reseller(str(user_id))
    
    if user is not None:
        return True
        
    return False


async def notify_owner(text: str):
    try: 
        await bot.send_message(chat_id=OWNER_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception as e: 
        print(f" Owner ထံသို့ Message ပို့၍မရပါ: {e}")


def get_random_proxy():
    if WEBSHARE_PROXIES:
        proxy_url = random.choice(WEBSHARE_PROXIES)
        return {"http": proxy_url, "https": proxy_url}
        
    return None

# ==========================================
# 4. Scraper Logic
# ==========================================
async def get_main_scraper():
    global GLOBAL_SCRAPER, GLOBAL_COOKIE_STR, GLOBAL_CSRF
    
    raw_cookie = await db.get_main_cookie() or ""
    
    if GLOBAL_SCRAPER is None or raw_cookie != GLOBAL_COOKIE_STR:
        cookie_dict = {}
        
        if raw_cookie:
            for item in raw_cookie.split(';'):
                if '=' in item:
                    k, v = item.strip().split('=', 1)
                    cookie_dict[k.strip()] = v.strip()
                    
        proxy_dict = get_random_proxy()
        
        GLOBAL_SCRAPER = AsyncSession(
            impersonate="chrome124", 
            cookies=cookie_dict,
            proxies=proxy_dict
        )
        GLOBAL_COOKIE_STR = raw_cookie
        GLOBAL_CSRF = {'mlbb_br': None, 'mlbb_ph': None, 'mcc_br': None, 'mcc_ph': None}
        
    return GLOBAL_SCRAPER


def _sync_drission_login(email, password):
    try:
        co = ChromiumOptions()
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-setuid-sandbox')
        co.set_argument('--disable-blink-features=AutomationControlled')
        co.set_user_agent("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36")
        
        co.headless(True) 

        page = ChromiumPage(co)
        page.get("https://www.smile.one/customer/login")
        page.wait(5)
        
        sign_in_btn = page.ele('text=Sign in with Google')
        
        if sign_in_btn:
            sign_in_btn.click()
        
        page.wait.new_tab()
        google_tab = page.get_tab(page.latest_tab)
        
        google_tab.wait(2)
        google_tab.ele('input[type="email"]').input(email)
        google_tab.wait(1)
        google_tab.ele('input[type="email"]').type('\n') 
        
        google_tab.wait(4)
        google_tab.ele('input[type="password"]').input(password)
        google_tab.wait(1)
        google_tab.ele('input[type="password"]').type('\n') 
        
        page.wait.url_change("customer/order", timeout=30)
        
        cookies_dict = page.cookies(as_dict=True)
        raw_cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
        
        page.quit()
        return raw_cookie_str
        
    except Exception as e:
        print(f"DrissionPage Login Error: {e}")
        try:
            page.quit()
        except:
            pass
            
        return None


async def auto_login_and_get_cookie():
    global last_login_time, GLOBAL_SCRAPER, GLOBAL_CSRF
    
    if not GOOGLE_EMAIL or not GOOGLE_PASS:
        print("❌ GOOGLE_EMAIL and GOOGLE_PASS are missing in .env.")
        return False
        
    async with auth_lock:
        if time.time() - last_login_time < 120:
            return True

        print("Logging in with Google to fetch new Cookie using DrissionPage...")
        
        loop = asyncio.get_running_loop()
        new_cookie_str = await loop.run_in_executor(None, _sync_drission_login, GOOGLE_EMAIL, GOOGLE_PASS)
        
        if new_cookie_str:
            print("✅ Auto-Login (Google) successful. Saving Cookie...")
            await db.update_main_cookie(new_cookie_str)
            last_login_time = time.time()
            GLOBAL_SCRAPER = None
            GLOBAL_CSRF = {'mlbb_br': None, 'mlbb_ph': None, 'mcc_br': None, 'mcc_ph': None}
            return True
            
        else:
            print("❌ Did not reach the Order page. (Google blocked or Checkpoint)")
            return False


async def get_smile_balance(scraper, headers, balance_url='https://www.smile.one/customer/order'):
    balances = {'br_balance': 0.00, 'ph_balance': 0.00}
    
    try:
        response = await scraper.get(balance_url, headers=headers, timeout=15)
        
        br_match = re.search(r'(?i)(?:Balance|Saldo)[\s:]*?<\/p>\s*<p>\s*([\d\.,]+)', response.text)
        if br_match:
            balances['br_balance'] = float(br_match.group(1).replace(',', ''))
        else:
            soup = BeautifulSoup(response.text, 'html.parser')
            main_balance_div = soup.find('div', class_='balance-coins')
            if main_balance_div:
                p_tags = main_balance_div.find_all('p')
                if len(p_tags) >= 2: 
                    balances['br_balance'] = float(p_tags[1].text.strip().replace(',', ''))
                    
        ph_match = re.search(r'(?i)Saldo PH[\s:]*?<\/span>\s*<span>\s*([\d\.,]+)', response.text)
        if ph_match:
            balances['ph_balance'] = float(ph_match.group(1).replace(',', ''))
        else:
            soup = BeautifulSoup(response.text, 'html.parser')
            ph_balance_container = soup.find('div', id='all-balance')
            if ph_balance_container:
                span_tags = ph_balance_container.find_all('span')
                if len(span_tags) >= 2:
                    balances['ph_balance'] = float(span_tags[1].text.strip().replace(',', ''))
                    
    except Exception as e: 
        print(f"Error fetching balance from site: {e}")
        
    return balances




async def get_verified_smile_balance(scraper, headers, balance_url, region, retries=3):
    """Fetch a real, parseable Smile.one balance.

    Returns (balance, True) only when the requested region balance can be
    parsed from the authenticated page. A missing/redirected/error page is
    never treated as zero, because zero could cause a false balance delta.
    """
    key = 'br_balance' if region == 'BR' else 'ph_balance'
    for attempt in range(retries):
        try:
            response = await scraper.get(
                f"{balance_url}{'&' if '?' in balance_url else '?'}_verify={int(time.time()*1000)}",
                headers=headers,
                timeout=15,
            )
            if 'login' in str(response.url).lower() or response.status_code in (401, 403, 503):
                return None, False

            balances = await get_smile_balance(scraper, headers, str(response.url))
            # get_smile_balance returns zero for both a genuine zero balance
            # and a parsing failure, so require the balance marker to exist.
            text = response.text or ''
            if region == 'BR':
                parsed = bool(re.search(r'(?i)(?:Balance|Saldo)[\s:]*?</p>\s*<p>\s*[\d\.,]+', text)) or bool(BeautifulSoup(text, 'html.parser').find('div', class_='balance-coins'))
            else:
                parsed = bool(re.search(r'(?i)Saldo PH[\s:]*?</span>\s*<span>\s*[\d\.,]+', text)) or bool(BeautifulSoup(text, 'html.parser').find('div', id='all-balance'))

            if parsed:
                return float(balances[key]), True
        except Exception as e:
            print(f"Balance verification attempt {attempt + 1}/{retries} failed: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(2)
    return None, False


async def process_smile_one_order_br(game_id, zone_id, product_id, currency_name="BR", prev_context=None, skip_role_check=False, known_ig_name="Unknown", last_success_order_id=""):
    scraper = await get_main_scraper()
    global GLOBAL_CSRF
    cache_key = "mlbb_br"

    main_url = 'https://www.smile.one/merchant/mobilelegends'
    checkrole_url = 'https://www.smile.one/merchant/mobilelegends/checkrole'
    query_url = 'https://www.smile.one/merchant/mobilelegends/query'
    pay_url = 'https://www.smile.one/merchant/mobilelegends/pay'
    order_api_url = 'https://www.smile.one/customer/activationcode/codelist'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest', 
        'Referer': main_url, 
        'Origin': 'https://www.smile.one'
    }

    try:
        csrf_token = prev_context.get('csrf_token') if prev_context else GLOBAL_CSRF.get(cache_key)
        ig_name = known_ig_name

        if not csrf_token:
            response = await scraper.get(main_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            meta_tag = soup.find('meta', {'name': 'csrf-token'})
            
            if meta_tag:
                csrf_token = meta_tag.get('content')
            elif soup.find('input', {'name': '_csrf'}):
                csrf_token = soup.find('input', {'name': '_csrf'}).get('value')
            else:
                csrf_token = None
                
            if not csrf_token:
                return {"status": "error", "message": "CSRF Token not found. Re-add Cookie.", "ig_name": ig_name}
                
            GLOBAL_CSRF[cache_key] = csrf_token

        async def get_flow_id():
            query_data = {
                'user_id': game_id,
                'zone_id': zone_id, 
                'pid': product_id, 
                'checkrole': '', 
                'pay_methond': 'smilecoin', 
                'channel_method': 'smilecoin', 
                '_csrf': csrf_token
            }
            return await scraper.post(query_url, data=query_data, headers=headers)

        async def check_role():
            check_data = {
                'user_id': game_id, 
                'zone_id': zone_id, 
                '_csrf': csrf_token
            }
            return await scraper.post(checkrole_url, data=check_data, headers=headers)

        if skip_role_check:
            query_response_raw = await get_flow_id()
        else:
            query_response_raw, role_response_raw = await asyncio.gather(get_flow_id(), check_role())
            try:
                role_result = role_response_raw.json()
                fetched_name = role_result.get('username') or role_result.get('data', {}).get('username')
                if fetched_name and str(fetched_name).strip() != "":
                    ig_name = str(fetched_name).strip()
                else:
                    return {"status": "error", "message": "❌ Invalid Account: Account not found.", "ig_name": "Unknown"}
            except Exception: 
                return {"status": "error", "message": "Check Role API Error.", "ig_name": ig_name}

        try:
            query_result = query_response_raw.json()
        except Exception:
            return {"status": "error", "message": "Query API Error", "ig_name": ig_name}
            
        flowid = query_result.get('flowid') or query_result.get('data', {}).get('flowid')
        
        if not flowid:
            real_error = query_result.get('msg') or query_result.get('message') or query_result.get('info') or ""
            
            if "login" in str(real_error).lower() or "unauthorized" in str(real_error).lower():
                GLOBAL_CSRF[cache_key] = None
                await notify_owner("⚠️ <b>Order Alert:</b> Cookie expired. Auto-login started...")
                success = await auto_login_and_get_cookie()
                
                if success:
                    return {"status": "error", "message": "Session renewed. Please try again.", "ig_name": ig_name}
                else:
                    return {"status": "error", "message": "❌ Auto-Login failed. Please /setcookie.", "ig_name": ig_name}
                
            return {"status": "error", "message": str(real_error), "ig_name": ig_name}

        pay_data = {
            '_csrf': csrf_token, 
            'user_id': game_id, 
            'zone_id': zone_id, 
            'pay_methond': 'smilecoin', 
            'product_id': product_id, 
            'channel_method': 'smilecoin', 
            'flowid': flowid, 
            'email': '', 
            'coupon_id': ''
        }
        pay_response_raw = await scraper.post(pay_url, data=pay_data, headers=headers)
        pay_text = pay_response_raw.text.lower()
        
        if "saldo insuficiente" in pay_text or "insufficient" in pay_text:
            return {"status": "error", "message": "Insufficient Balance.", "ig_name": ig_name}
        
        real_order_id = "Not found"
        is_success = False
        actual_product_name = ""

        try:
            pay_json = pay_response_raw.json()
            status_val = str(pay_json.get('status', ''))
            code = str(pay_json.get('code', status_val))
            msg = str(pay_json.get('msg') or pay_json.get('message') or pay_json.get('info') or "").lower()
            
            if code in ['200', '0', '1'] or 'success' in msg: 
                is_success = True
                _id = str(pay_json.get('data', {}).get('order_id') or pay_json.get('order_id') or pay_json.get('increment_id') or "")
                
                if not _id or _id == "None":
                    _id = f"FAST_{int(time.time())}_{random.randint(100,999)}"
                    
                real_order_id = _id
        except:
            if 'success' in pay_text or 'sucesso' in pay_text: 
                is_success = True
                real_order_id = f"FAST_{int(time.time())}_{random.randint(100,999)}"

        if not is_success:
            try:
                hist_res_raw = await scraper.get(order_api_url, params={'type': 'orderlist', 'p': '1', 'pageSize': '5'}, headers=headers)
                hist_json = hist_res_raw.json()
                
                if 'list' in hist_json and len(hist_json['list']) > 0:
                    for order in hist_json['list']:
                        if str(order.get('user_id')) == str(game_id) and str(order.get('server_id')) == str(zone_id):
                            current_order_id = str(order.get('increment_id', ""))
                            if current_order_id != last_success_order_id:
                                if str(order.get('order_status', '')).lower() in ['success', '1'] or str(order.get('status')) == '1':
                                    real_order_id = current_order_id
                                    actual_product_name = str(order.get('product_name', ''))
                                    is_success = True
                                    break
            except:
                pass

        if is_success:
            return {
                "status": "success", 
                "ig_name": ig_name, 
                "order_id": real_order_id, 
                "csrf_token": csrf_token, 
                "product_name": actual_product_name
            }
        else:
            error_detail = pay_json.get('msg') or pay_json.get('message') or pay_json.get('info') if 'pay_json' in locals() else "Payment Verification Failed."
            return {"status": "error", "message": str(error_detail), "ig_name": ig_name}

    except Exception as e: 
        return {"status": "error", "message": f"System Error: {str(e)}", "ig_name": known_ig_name}


async def process_smile_one_order_ph(game_id, zone_id, product_id, currency_name="PH", prev_context=None, skip_role_check=False, known_ig_name="Unknown", last_success_order_id=""):
    scraper = await get_main_scraper()
    global GLOBAL_CSRF
    cache_key = "mlbb_ph"

    main_url = 'https://www.smile.one/ph/merchant/mobilelegends'
    checkrole_url = 'https://www.smile.one/ph/merchant/mobilelegends/checkrole'
    query_url = 'https://www.smile.one/ph/merchant/mobilelegends/query'
    pay_url = 'https://www.smile.one/ph/merchant/mobilelegends/pay'
    order_api_url = 'https://www.smile.one/ph/customer/activationcode/codelist'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest', 
        'Referer': main_url, 
        'Origin': 'https://www.smile.one'
    }

    try:
        csrf_token = prev_context.get('csrf_token') if prev_context else GLOBAL_CSRF.get(cache_key)
        ig_name = known_ig_name

        if not csrf_token:
            response = await scraper.get(main_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            meta_tag = soup.find('meta', {'name': 'csrf-token'})
            
            if meta_tag:
                csrf_token = meta_tag.get('content')
            elif soup.find('input', {'name': '_csrf'}):
                csrf_token = soup.find('input', {'name': '_csrf'}).get('value')
            else:
                csrf_token = None
                
            if not csrf_token:
                return {"status": "error", "message": "CSRF Token not found. Re-add Cookie.", "ig_name": ig_name}
                
            GLOBAL_CSRF[cache_key] = csrf_token

        async def get_flow_id():
            query_data = {
                'user_id': game_id,
                'zone_id': zone_id, 
                'pid': product_id, 
                'checkrole': '', 
                'pay_methond': 'smilecoin', 
                'channel_method': 'smilecoin', 
                '_csrf': csrf_token
            }
            return await scraper.post(query_url, data=query_data, headers=headers)

        async def check_role():
            check_data = {
                'user_id': game_id, 
                'zone_id': zone_id, 
                '_csrf': csrf_token
            }
            return await scraper.post(checkrole_url, data=check_data, headers=headers)

        if skip_role_check:
            query_response_raw = await get_flow_id()
        else:
            query_response_raw, role_response_raw = await asyncio.gather(get_flow_id(), check_role())
            try:
                role_result = role_response_raw.json()
                fetched_name = role_result.get('username') or role_result.get('data', {}).get('username')
                if fetched_name and str(fetched_name).strip() != "":
                    ig_name = str(fetched_name).strip()
                else:
                    return {"status": "error", "message": "❌ Invalid Account: Account not found.", "ig_name": "Unknown"}
            except Exception: 
                return {"status": "error", "message": "Check Role API Error.", "ig_name": ig_name}

        try:
            query_result = query_response_raw.json()
        except Exception:
            return {"status": "error", "message": "Query API Error", "ig_name": ig_name}
            
        flowid = query_result.get('flowid') or query_result.get('data', {}).get('flowid')
        
        if not flowid:
            real_error = query_result.get('msg') or query_result.get('message') or query_result.get('info') or ""
            
            if "login" in str(real_error).lower() or "unauthorized" in str(real_error).lower():
                GLOBAL_CSRF[cache_key] = None
                await notify_owner("⚠️ <b>Order Alert:</b> Cookie expired. Auto-login started...")
                success = await auto_login_and_get_cookie()
                
                if success:
                    return {"status": "error", "message": "Session renewed. Please try again.", "ig_name": ig_name}
                else:
                    return {"status": "error", "message": "❌ Auto-Login failed. Please /setcookie.", "ig_name": ig_name}
                
            return {"status": "error", "message": str(real_error), "ig_name": ig_name}

        pay_data = {
            '_csrf': csrf_token, 
            'user_id': game_id, 
            'zone_id': zone_id, 
            'pay_methond': 'smilecoin', 
            'product_id': product_id, 
            'channel_method': 'smilecoin', 
            'flowid': flowid, 
            'email': '', 
            'coupon_id': ''
        }
        pay_response_raw = await scraper.post(pay_url, data=pay_data, headers=headers)
        pay_text = pay_response_raw.text.lower()
        
        if "saldo insuficiente" in pay_text or "insufficient" in pay_text:
            return {"status": "error", "message": "Insufficient Balance.", "ig_name": ig_name}
        
        real_order_id = "Not found"
        is_success = False
        actual_product_name = ""

        try:
            pay_json = pay_response_raw.json()
            status_val = str(pay_json.get('status', ''))
            code = str(pay_json.get('code', status_val))
            msg = str(pay_json.get('msg') or pay_json.get('message') or pay_json.get('info') or "").lower()
            
            if code in ['200', '0', '1'] or 'success' in msg: 
                is_success = True
                _id = str(pay_json.get('data', {}).get('order_id') or pay_json.get('order_id') or pay_json.get('increment_id') or "")
                
                if not _id or _id == "None":
                    _id = f"FAST_{int(time.time())}_{random.randint(100,999)}"
                    
                real_order_id = _id
        except:
            if 'success' in pay_text or 'sucesso' in pay_text: 
                is_success = True
                real_order_id = f"FAST_{int(time.time())}_{random.randint(100,999)}"

        if not is_success:
            try:
                hist_res_raw = await scraper.get(order_api_url, params={'type': 'orderlist', 'p': '1', 'pageSize': '5'}, headers=headers)
                hist_json = hist_res_raw.json()
                
                if 'list' in hist_json and len(hist_json['list']) > 0:
                    for order in hist_json['list']:
                        if str(order.get('user_id')) == str(game_id) and str(order.get('server_id')) == str(zone_id):
                            current_order_id = str(order.get('increment_id', ""))
                            if current_order_id != last_success_order_id:
                                if str(order.get('order_status', '')).lower() in ['success', '1'] or str(order.get('status')) == '1':
                                    real_order_id = current_order_id
                                    actual_product_name = str(order.get('product_name', ''))
                                    is_success = True
                                    break
            except:
                pass

        if is_success:
            return {
                "status": "success", 
                "ig_name": ig_name, 
                "order_id": real_order_id, 
                "csrf_token": csrf_token, 
                "product_name": actual_product_name
            }
        else:
            error_detail = pay_json.get('msg') or pay_json.get('message') or pay_json.get('info') if 'pay_json' in locals() else "Payment Verification Failed."
            return {"status": "error", "message": str(error_detail), "ig_name": ig_name}

    except Exception as e: 
        return {"status": "error", "message": f"System Error: {str(e)}", "ig_name": known_ig_name}


async def process_mcc_order(game_id, zone_id, product_id, currency_name, prev_context=None, skip_role_check=False, known_ig_name="Unknown", last_success_order_id=""):
    scraper = await get_main_scraper()
    global GLOBAL_CSRF
    cache_key = f"mcc_{currency_name.lower()}"

    if currency_name == 'PH':
        main_url = 'https://www.smile.one/ph/merchant/game/magicchessgogo'
        checkrole_url = 'https://www.smile.one/ph/merchant/game/checkrole'
        query_url = 'https://www.smile.one/ph/merchant/game/createorder' 
        pay_url = 'https://www.smile.one/ph/merchant/game/pay' 
        order_api_url = 'https://www.smile.one/ph/customer/activationcode/codelist'
    else:
        main_url = 'https://www.smile.one/br/merchant/game/magicchessgogo'
        checkrole_url = 'https://www.smile.one/br/merchant/game/checkrole'
        query_url = 'https://www.smile.one/br/merchant/game/createorder' 
        pay_url = 'https://www.smile.one/br/merchant/game/pay'
        order_api_url = 'https://www.smile.one/br/customer/activationcode/codelist'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest', 
        'Referer': main_url, 
        'Origin': 'https://www.smile.one'
    }

    try:
        csrf_token = prev_context.get('csrf_token') if prev_context else GLOBAL_CSRF.get(cache_key)
        ig_name = known_ig_name
        
        if not csrf_token:
            response = await scraper.get(main_url, headers=headers)
            
            if response.status_code in [403, 503] or "cloudflare" in response.text.lower():
                 return {"status": "error", "message": "Blocked by Cloudflare.", "ig_name": ig_name}

            soup = BeautifulSoup(response.text, 'html.parser')
            meta_tag = soup.find('meta', {'name': 'csrf-token'})
            
            if meta_tag:
                csrf_token = meta_tag.get('content')
            elif soup.find('input', {'name': '_csrf'}):
                csrf_token = soup.find('input', {'name': '_csrf'}).get('value')
            else:
                csrf_token = None
                
            if not csrf_token:
                return {"status": "error", "message": "CSRF Token not found. Add a new Cookie using /setcookie.", "ig_name": ig_name}
                
            GLOBAL_CSRF[cache_key] = csrf_token

        async def get_flow_id():
            query_data = {
                'uid': game_id,
                'sid': zone_id,
                'productid': product_id,
                'channel_method': 'smilecoin',
                'external': 'false',
                '_csrf': csrf_token
            }
            return await scraper.post(query_url, params={'product': 'magicchessgogo'}, data=query_data, headers=headers)

        async def check_role():
            check_data = {
                'uid': game_id,
                'sid': zone_id,
                'checkrole': '1',
                'product': 'magicchessgogo',
                '_csrf': csrf_token
            }
            return await scraper.post(checkrole_url, params={'product': 'magicchessgogo'}, data=check_data, headers=headers)

        if skip_role_check:
            query_response_raw = await get_flow_id()
        else:
            query_response_raw, role_response_raw = await asyncio.gather(get_flow_id(), check_role())
            try:
                role_result = role_response_raw.json()
                fetched_name = role_result.get('nickname') or role_result.get('username') or role_result.get('role_name') or role_result.get('data', {}).get('nickname') or role_result.get('data', {}).get('username')
                
                if fetched_name and str(fetched_name).strip() != "":
                    ig_name = str(fetched_name).strip()
                else:
                    ig_name = "Unknown" 
            except Exception: 
                ig_name = "Unknown"

        try: 
            query_result = query_response_raw.json()
        except Exception: 
            return {"status": "error", "message": "Query API Error: Failed to load JSON.", "ig_name": ig_name}
            
        flowid = query_result.get('flowid') or query_result.get('data', {}).get('flowid')
        
        if not flowid:
            real_error = query_result.get('msg') or query_result.get('message') or query_result.get('info') or ""
            
            if "login" in str(real_error).lower() or "unauthorized" in str(real_error).lower():
                GLOBAL_CSRF[cache_key] = None
                await notify_owner("⚠️ <b>Order Alert:</b> Cookie expired. Auto-login started...")
                success = await auto_login_and_get_cookie()
                
                if success:
                    return {"status": "error", "message": "Session renewed. Please enter the command again.", "ig_name": ig_name}
                else: 
                    return {"status": "error", "message": "❌ Auto-Login failed. Please provide /setcookie.", "ig_name": ig_name}
            
            error_display = str(real_error) if real_error else "Invalid account or unable to purchase."
            return {"status": "error", "message": error_display, "ig_name": ig_name}

        pay_data = {
            '_csrf': csrf_token,
            'uid': game_id,
            'sid': zone_id,
            'email': '',
            'pay_methond': 'smilecoin',
            'channel_method': 'smilecoin',
            'flowid': flowid,
            'pay_country': '',
            'coupon_id': '',
            'zipcode': '',
            'product': 'magicchessgogo',
            'productid': product_id,
            'external': 'false'
        }
        
        pay_headers = headers.copy()
        if 'X-Requested-With' in pay_headers:
            del pay_headers['X-Requested-With'] 
            
        pay_headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'

        pay_response_raw = await scraper.post(pay_url, data=pay_data, headers=pay_headers, allow_redirects=False)
        
        status_code = pay_response_raw.status_code
        location = str(pay_response_raw.headers.get('Location') or pay_response_raw.headers.get('location') or "")
        pay_text = pay_response_raw.text.strip().lower()
        
        if "saldo insuficiente" in pay_text or "insufficient" in pay_text:
            return {"status": "error", "message": "Insufficient Balance.", "ig_name": ig_name}
        
        real_order_id = "Not found"
        is_success = False
        actual_product_name = ""

        if status_code in [301, 302, 303]:
            if "customer/order" in location or "success" in location or "pay" in location:
                is_success = True
                real_order_id = f"FAST_{int(time.time())}_{random.randint(100,999)}"
        
        if not is_success and pay_text:
            try:
                pay_json = pay_response_raw.json()
                code = str(pay_json.get('code', pay_json.get('status', '')))
                msg = str(pay_json.get('msg') or pay_json.get('message') or pay_json.get('info') or "").lower()
                
                if code in ['200', '0', '1'] or 'success' in msg: 
                    is_success = True
                    _id = str(pay_json.get('data', {}).get('order_id') or pay_json.get('order_id') or pay_json.get('increment_id') or "")
                    
                    if not _id or _id == "None":
                        _id = f"FAST_{int(time.time())}_{random.randint(100,999)}"
                        
                    real_order_id = _id
            except:
                if 'success' in pay_text or 'sucesso' in pay_text: 
                    is_success = True
                    real_order_id = f"FAST_{int(time.time())}_{random.randint(100,999)}"

        if not is_success:
            try:
                hist_res_raw = await scraper.get(order_api_url, params={'type': 'orderlist', 'p': '1', 'pageSize': '5'}, headers=headers)
                hist_json = hist_res_raw.json()
                
                if 'list' in hist_json and len(hist_json['list']) > 0:
                    for order in hist_json['list']:
                        uid_val = str(order.get('user_id') or order.get('uid') or "")
                        sid_val = str(order.get('server_id') or order.get('sid') or order.get('zone_id') or "")
                        
                        if uid_val == str(game_id) and sid_val == str(zone_id):
                            current_order_id = str(order.get('increment_id', ""))
                            if current_order_id != last_success_order_id:
                                if str(order.get('order_status', '')).lower() in ['success', '1'] or str(order.get('status')) == '1':
                                    real_order_id = current_order_id
                                    actual_product_name = str(order.get('product_name', ''))
                                    is_success = True
                                    break
            except Exception: 
                pass

        if is_success:
            return {
                "status": "success", 
                "ig_name": ig_name, 
                "order_id": real_order_id, 
                "csrf_token": csrf_token, 
                "product_name": actual_product_name
            }
        else:
            if status_code in [301, 302, 303]:
                error_detail = "Payment Rejected by Server (Invalid Item or Region Mismatch)"
                
                if "error" in location:
                    try:
                        err_url = location if location.startswith('http') else f"https://www.smile.one{location}"
                        err_res = await scraper.get(err_url, headers=headers)
                        err_soup = BeautifulSoup(err_res.text, 'html.parser')
                        msg_box = err_soup.find(class_=re.compile('msg|error-message', re.I))
                        if msg_box:
                            error_detail = f"Declined: {msg_box.text.strip()}"
                    except: 
                        pass
                        
            elif not pay_text:
                error_detail = f"Empty Response (HTTP {status_code})"
            else:
                error_detail = f"Reply: {pay_text[:80]}..."
                
            return {"status": "error", "message": error_detail, "ig_name": ig_name}

    except Exception as e: 
        return {"status": "error", "message": f"System Error: {str(e)}", "ig_name": known_ig_name}


# ==========================================
# 5. Message Handlers
# ==========================================
async def execute_buy_process(message, lines, regex_pattern, currency, packages_dict, process_func, title_prefix, is_mcc=False):
    tg_id = str(message.from_user.id)
    telegram_user = message.from_user.username
    
    # Header အတွက် Username သို့မဟုတ် First Name ကို ရယူမည်
    display_uname = telegram_user if telegram_user else (message.from_user.first_name or str(tg_id))
        
    async with user_locks[tg_id], smile_account_lock:
        parsed_orders = []
        
        for line in lines:
            line = line.strip()
            
            if not line: 
                continue 
            
            match = re.search(regex_pattern, line)
            if not match:
                await message.reply(
                    f"Invalid format: `{line}`\nCheck /help for correct format.",
                    link_preview_options=types.LinkPreviewOptions(is_disabled=True)
                )
                continue
                
            game_id = match.group(1)
            zone_id = match.group(2)
            raw_items_str = match.group(3).lower()
            
            requested_packages = raw_items_str.split()
            packages_to_buy = []
            not_found_pkgs = []
            
            for pkg in requested_packages:
                active_packages = None
                
                if isinstance(packages_dict, list):
                    for p_dict in packages_dict:
                        if pkg in p_dict: 
                            active_packages = p_dict
                            break
                else:
                    if pkg in packages_dict: 
                        active_packages = packages_dict
                        
                if active_packages: 
                    pkg_items = []
                    for item_dict in active_packages[pkg]:
                        new_item = item_dict.copy()
                        new_item['pkg_name'] = pkg.upper() 
                        pkg_items.append(new_item)
                        
                    packages_to_buy.append({
                        'pkg_name': pkg.upper(), 
                        'items': pkg_items
                    })
                else: 
                    not_found_pkgs.append(pkg)
                    
            if not_found_pkgs:
                await message.reply(
                    f"❌ Package(s) not found for ID {game_id}: {', '.join(not_found_pkgs)}",
                    link_preview_options=types.LinkPreviewOptions(is_disabled=True)
                )
                continue
                
            if not packages_to_buy: 
                continue
                
            line_price = sum(item['price'] for p in packages_to_buy for item in p['items'])
            
            parsed_orders.append({
                'game_id': game_id, 
                'zone_id': zone_id, 
                'raw_items_str': raw_items_str, 
                'packages_to_buy': packages_to_buy, 
                'line_price': line_price
            })
            
        if not parsed_orders: 
            return
            
        start_time = time.time()
        loading_icon = "<tg-emoji emoji-id='5895403643863043222'>🫧</tg-emoji>"
        loading_msg = await message.reply(
            f"{loading_icon}",
            link_preview_options=types.LinkPreviewOptions(is_disabled=True)
        )

        # ① User Wallet Balance စစ်
        total_required_amount = sum(order['line_price'] for order in parsed_orders)
        user_wallet_balance = await db.get_user_balance(tg_id)

        if user_wallet_balance is None:
            await loading_msg.delete()
            return await message.reply("❌ User wallet not found.")

        if user_wallet_balance < total_required_amount:
            await loading_msg.delete()
            needed_amount = total_required_amount - user_wallet_balance
            error_msg = (
                f"✖ <b>INSUFFICIENT WALLET BALANCE</b>\n\n"
                f"<b><code>COST : {total_required_amount:,.2f} 🪙</code></b>\n"
                f"<b><code>BALANCE : {user_wallet_balance:,.2f} 🪙</code></b>\n"
                f"<b><code>NEED : {needed_amount:,.2f} 🪙</code></b>\n"
                f"<code>━━━━━━━━━━━━━━━━━━</code>"
            )
            return await message.reply(error_msg, parse_mode=ParseMode.HTML)

        # ② Official Smile.one Account Balance စစ်
        scraper = await get_main_scraper()
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://www.smile.one'
        }

        try:
            bals_before = await get_smile_balance(scraper, headers)
            if currency == 'BR':
                initial_bal_for_receipt = bals_before['br_balance']
            else:
                initial_bal_for_receipt = bals_before['ph_balance']
        except Exception:
            await loading_msg.delete()
            return await message.reply(
                "❌ Unable to verify official Smile.one balance. Purchase was not started and your wallet was not charged."
            )

        if initial_bal_for_receipt < total_required_amount:
            await loading_msg.delete()
            needed_amount = total_required_amount - initial_bal_for_receipt
            flag = f"<tg-emoji emoji-id='{BR_EMOJI}'>🇧🇷</tg-emoji>" if currency == 'BR' else f"<tg-emoji emoji-id='{PH_EMOJI}'>🇵🇭</tg-emoji>"
            error_msg = (
                f"✖ <b>INSUFFICIENT OFFICIAL BALANCE</b>\n\n"
                f"{flag} <b><code>COST : {total_required_amount:,.2f} 🪙</code></b>\n"
                f"{flag} <b><code>OFFICIAL BALANCE : {initial_bal_for_receipt:,.2f} 🪙</code></b>\n"
                f"{flag} <b><code>NEED : {needed_amount:,.2f} 🪙</code></b>\n"
                f"<code>━━━━━━━━━━━━━━━━━━</code>\n"
                f"Your wallet was not charged."
            )
            return await message.reply(error_msg, parse_mode=ParseMode.HTML)

        current_official_bal = [initial_bal_for_receipt] 

        async def process_order_line(order):
            game_id = order['game_id']
            zone_id = order.get('order_zone', order['zone_id'])
            raw_items_str = order['raw_items_str']
            packages_to_buy = order['packages_to_buy']
            
            overall_success_count = 0
            overall_fail_count = 0
            total_spent = 0.0
            ig_name = "Unknown"
            package_results = []

            async with api_semaphore:
                prev_context = None
                last_success_order = ""
                
                for pkg_data in packages_to_buy:
                    pkg_name = pkg_data['pkg_name']
                    items = pkg_data['items']
                    
                    pkg_success_count = 0
                    pkg_fail_count = 0
                    pkg_spent = 0.0
                    pkg_order_ids = ""
                    pkg_error = ""
                    
                    pkg_total_price = sum(item['price'] for item in items)
                    
                    if current_official_bal[0] < pkg_total_price:
                        pkg_fail_count = len(items)
                        pkg_error = "Insufficient Balance"
                        overall_fail_count += 1
                        package_results.append({
                            'pkg_name': pkg_name, 
                            'status': 'fail', 
                            'spent': 0.0, 
                            'order_ids': "", 
                            'error_msg': pkg_error, 
                            'ig_name': ig_name
                        })
                        continue

                    for item in items:
                        if current_official_bal[0] < item['price']:
                            pkg_fail_count += 1
                            pkg_error = "Insufficient Balance"
                            break

                        current_official_bal[0] -= item['price']
                        
                        skip_check = False
                        res = {}
                        max_retries = 3
                        
                        for attempt in range(max_retries):
                            res = await process_func(
                                game_id, 
                                zone_id, 
                                item['pid'], 
                                currency, 
                                prev_context=prev_context, 
                                skip_role_check=skip_check, 
                                known_ig_name=ig_name, 
                                last_success_order_id=last_success_order
                            )
                            
                            error_text_check = str(res.get('message', '')).lower()
                            
                            if (res.get('status') == 'success' or 
                                "insufficient" in error_text_check or 
                                "invalid" in error_text_check or 
                                "not found" in error_text_check or 
                                "limit" in error_text_check or 
                                "exceed" in error_text_check or 
                                "máximo" in error_text_check):
                                break
                                
                            if attempt < max_retries - 1:
                                if ("erro no servidor" in error_text_check or 
                                    "server error" in error_text_check or 
                                    "cloudflare" in error_text_check or 
                                    "query failed" in error_text_check):
                                    await asyncio.sleep(5.0)
                                else: 
                                    await asyncio.sleep(2.0)
                                
                        fetched_name = res.get('ig_name') or res.get('username') or res.get('role_name') or res.get('nickname')
                        if fetched_name and str(fetched_name).strip() not in ["", "Unknown", "None"]:
                            ig_name = str(fetched_name).strip()

                        if res.get('status') == 'success':
                            pkg_success_count += 1
                            pkg_spent += item['price']
                            pkg_order_ids += f"{res.get('order_id', '')}\n"
                            prev_context = {'csrf_token': res.get('csrf_token')}
                            last_success_order = res.get('order_id', '')
                        else:
                            current_official_bal[0] += item['price'] 
                            pkg_fail_count += 1
                            pkg_error = res.get('message', 'Unknown Error')
                            break 
                            
                    if pkg_success_count > 0:
                        overall_success_count += 1
                        total_spent += pkg_spent
                        display_name = pkg_name
                        
                        if len(items) > 1 and pkg_success_count < len(items):
                            if pkg_name.upper().startswith("WP"):
                                display_name = f"WP{pkg_success_count}"
                            else:
                                display_name = f"{pkg_name} ({pkg_success_count}/{len(items)} Success)"
                                
                        package_results.append({
                            'pkg_name': display_name, 
                            'status': 'success', 
                            'spent': pkg_spent, 
                            'order_ids': pkg_order_ids.strip(), 
                            'error_msg': "", 
                            'ig_name': ig_name
                        })
                        
                    if pkg_fail_count > 0:
                        overall_fail_count += 1
                        display_name = pkg_name
                        
                        if len(items) > 1 and pkg_fail_count < len(items):
                            if pkg_name.upper().startswith("WP"):
                                display_name = f"WP{len(items) - pkg_success_count}"
                            else:
                                display_name = f"{pkg_name} ({len(items) - pkg_success_count} Failed)"
                                
                        package_results.append({
                            'pkg_name': display_name, 
                            'status': 'fail', 
                            'spent': 0.0, 
                            'order_ids': "", 
                            'error_msg': pkg_error, 
                            'ig_name': ig_name
                        })
                        
            return {
                'game_id': game_id, 
                'zone_id': zone_id, 
                'raw_items_str': raw_items_str, 
                'success_count': overall_success_count, 
                'fail_count': overall_fail_count, 
                'total_spent': total_spent, 
                'ig_name': ig_name, 
                'package_results': package_results
            }

        line_tasks = [process_order_line(order) for order in parsed_orders]
        line_results = await asyncio.gather(*line_tasks)
        time_taken_seconds = int(time.time() - start_time)

        # ④ Purchase Result စစ်ပြီး အောင်မြင်တဲ့ amount ကိုသာ User Wallet မှ ဖြတ်
        actual_wallet_charge = round(
            sum(float(res.get('total_spent', 0.0) or 0.0) for res in line_results),
            2
        )

        if actual_wallet_charge > 0:
            charged_ok, new_wallet_balance = await db.change_user_balance(
                tg_id, -actual_wallet_charge
            )
            if not charged_ok:
                await loading_msg.delete()
                return await message.reply(
                    "⚠️ Purchase processing completed, but the wallet charge could not be completed. Please contact the Owner before retrying."
                )

        await loading_msg.delete() 

        if not line_results: 
            return
            
        now = datetime.datetime.now(MMT) 
        # Date Format အသစ်: စက္ကန့်မပါဘဲ ရက်စွဲနှင့် အချိန်
        date_str = now.strftime("%d.%m.%Y-%I:%M%p")

        try:
            await asyncio.sleep(2) 
            anti_cache_url = f"https://www.smile.one/customer/order?_t={int(time.time())}"
            bals_after = await get_smile_balance(scraper, headers, anti_cache_url)
            
            if currency == 'BR':
                final_bal_for_receipt = bals_after['br_balance']
            else:
                final_bal_for_receipt = bals_after['ph_balance']
        except:
            final_bal_for_receipt = current_official_bal[0]


        # 🌟 Premium Emoji ပြန်သုံးခြင်း 🌟
        flag = f"<tg-emoji emoji-id='{BR_EMOJI}'>🇧🇷</tg-emoji>" if currency == 'BR' else f"<tg-emoji emoji-id='{PH_EMOJI}'>🇵🇭</tg-emoji>"
        
        # TRANSACTION REPORT အတွက် အစိမ်းရောင် Icon
        report_icon = "<tg-emoji emoji-id='5895403643863043222'>🟢</tg-emoji>"

        for res in line_results:
            report_lines = []
            
            report_lines.append(f"{report_icon}<code>TRANSACTION REPORT</code>")
            report_lines.append(f"<code>━━━━━━━━━━━━━━━━━━</code>")

            for pr in res['package_results']:
                safe_ig_name = html.escape(str(pr['ig_name']))
                pkg_display = f"{pr['pkg_name']}" if "WP" in pr['pkg_name'].upper() else f"{pr['pkg_name']} Diamonds"
                
                if pr['status'] == 'success':
                    report_lines.append(f"<code>Status : ✅ Sᴜᴄᴄᴇꜱꜱ</code>")
                    report_lines.append(f"<code>UID    : {res['game_id']} ({res['zone_id']})</code>")
                    report_lines.append(f"<code>Name   : {safe_ig_name}</code>")
                    report_lines.append(f"<code>Order  : {pkg_display}</code>")
                    
                    # Serial များကို တစ်ကြောင်းစီ ညီညာစွာ စီစဉ်ခြင်း
                    serials = [sn.strip() for sn in pr['order_ids'].split('\n') if sn.strip()]
                    if serials:
                        report_lines.append(f"<code>Serial : {serials[0]}</code>")
                        for sn in serials[1:]:
                            report_lines.append(f"<code>         {sn}</code>")
                            
                    report_lines.append(f"<code>Spent  : {pr['spent']:.2f} 🪙</code>")
                    
                    final_order_ids = pr['order_ids'].replace('\n', ', ')
                    
                    await db.save_order(
                        tg_id=tg_id, 
                        game_id=res['game_id'], 
                        zone_id=res['zone_id'], 
                        item_name=pr['pkg_name'], 
                        price=pr['spent'], 
                        order_id=final_order_ids, 
                        status="success"
                    )
                else:
                    error_text = str(pr['error_msg']).lower()
                    
                    if "insufficient" in error_text or "saldo" in error_text: 
                        display_err = "Insufficient Balance"
                    elif "invalid" in error_text or "not found" in error_text: 
                        display_err = "Invalid Account"
                    elif "erro no servidor" in error_text or "server error" in error_text: 
                        display_err = "Game Server Error"
                    elif "query failed" in error_text: 
                        display_err = "Smile.one API error"
                    elif "limit" in error_text or "exceed" in error_text or "máximo" in error_text or "limite" in error_text: 
                        display_err = "Weekly Pass Limit Exceeded"
                    elif "zone" in error_text or "region" in error_text or "country" in error_text or "indonesia" in error_text or "support recharge" in error_text or "singapore" in error_text or "russia" in error_text or "philippines" in error_text: 
                        display_err = "Ban Server"
                    else: 
                        display_err = pr['error_msg'].replace('❌', '').strip()
                        if not display_err: 
                            display_err = "Purchase Failed"
                            
                        if "wp" in pr['pkg_name'].lower():
                            if "unable" in error_text or "fail" in error_text or "error" in error_text: 
                                display_err = "Weekly Pass Limit Exceeded"
                                
                    report_lines.append(f"<code>Status : ❌ Fᴀɪʟᴇᴅ</code>")
                    report_lines.append(f"<code>UID    : {res['game_id']} ({res['zone_id']})</code>")
                    report_lines.append(f"<code>Name   : {safe_ig_name}</code>")
                    report_lines.append(f"<code>Order  : {pkg_display}</code>")
                    report_lines.append(f"<code>Error  : {display_err}</code>")
            report_lines.append(f"<code>Date   : {date_str}</code>")
            report_lines.append(f"<code>==== {display_uname} ====</code>")
            report_lines.append(f"{flag}<code>Before : {initial_bal_for_receipt:,.2f}</code>")
            report_lines.append(f"{flag}<code>Spent  : {res['total_spent']:,.2f}</code>")
            report_lines.append(f"{flag}<code>After  : {final_bal_for_receipt:,.2f}</code>")
            report_lines.append("")
            report_lines.append(f"<code>Success {res['success_count']} / Fᴀɪʟᴇᴅ {res['fail_count']}</code>")
            
            # List ထဲကစာကြောင်းတွေကို \n (Enter) နဲ့ပေါင်းပြီး Message အဖြစ်ပြောင်းခြင်း
            final_report = "\n".join(report_lines)
            
            await message.reply(
                final_report, 
                parse_mode=ParseMode.HTML,
                link_preview_options=types.LinkPreviewOptions(is_disabled=True)
            )










@dp.message(or_f(Command("role"), F.text.regexp(r"(?i)^\.role(?:$|\s+)")))
async def handle_check_role(message: types.Message):
    if not await is_authorized(message.from_user.id):
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")

    match = re.search(r"(?i)^[./]?role\s+(\d+)\s*[\(]?\s*(\d+)\s*[\)]?", message.text.strip())
    if not match:
        return await message.reply("❌ Invalid format. Use: `.role 12345678 1234`")

    game_id, zone_id = match.group(1).strip(), match.group(2).strip()
    loading_msg = await message.reply("Checking account data...", parse_mode=ParseMode.HTML)

    url_caliph = 'https://cekidml.caliph.dev/api/validasi'
    params_caliph = {'id': game_id, 'serverid': zone_id}
    headers_caliph = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://cekidml.caliph.dev/',
        'X-Requested-With': 'XMLHttpRequest'
    }

    url_malsawma = 'https://www.malsawmastore.in/gadget/doublediamonds_action.php'
    payload_malsawma = {'id': game_id, 'zone': zone_id}
    headers_malsawma = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
        'Origin': 'https://www.malsawmastore.in',
        'Referer': 'https://www.malsawmastore.in/gadget/doublediamonds',
        'Accept': 'application/json, text/javascript, */*; q=0.01'
    }

    try:
        async with AsyncSession(impersonate="safari_ios") as local_scraper:
            await local_scraper.get('https://cekidml.caliph.dev/', headers=headers_caliph, timeout=15)
            res_caliph, res_malsawma = await asyncio.gather(
                local_scraper.get(url_caliph, params=params_caliph, headers=headers_caliph, timeout=15),
                local_scraper.post(url_malsawma, data=payload_malsawma, headers=headers_malsawma, timeout=15)
            )

        ig_name = "Unknown"
        region = "Unknown"

        try:
            data_caliph = res_caliph.json()
            if data_caliph.get('status') == 'success':
                result_data = data_caliph.get('result', {})
                ig_name = result_data.get('nickname', 'Unknown')
                region = result_data.get('country', 'Unknown')
            else:
                error_msg = data_caliph.get('message') or data_caliph.get('msg') or "Game ID သို့မဟုတ် Zone ID မှားယွင်းနေပါသည်။"
                return await loading_msg.edit_text(f"❌ <b>Invalid Account:</b> {html.escape(str(error_msg))}", parse_mode=ParseMode.HTML)
        except Exception:
            debug_msg = res_caliph.text[:120].replace('<', '&lt;').replace('>', '&gt;').strip()
            return await loading_msg.edit_text(f"❌ <b>API Error:</b>\n<code>{debug_msg}...</code>", parse_mode=ParseMode.HTML)

        limit_50 = limit_150 = limit_250 = limit_500 = True
        debug_bonus_error = ""

        try:
            data_double = res_malsawma.json()
            if str(data_double.get('status', '')).lower() == 'true':
                dd_data = data_double.get('dd', {}) or {}
                limit_50 = not bool(dd_data.get('50', False))
                limit_150 = not bool(dd_data.get('150', False))
                limit_250 = not bool(dd_data.get('250', False))
                limit_500 = not bool(dd_data.get('500', False))
            else:
                debug_bonus_error = " <i>(Bonus Data Unavailable)</i>"
        except Exception:
            debug_bonus_error = " <i>(Bonus Data Error)</i>"

        style_50 = "danger" if limit_50 else "success"
        style_150 = "danger" if limit_150 else "success"
        style_250 = "danger" if limit_250 else "success"
        style_500 = "danger" if limit_500 else "success"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Bᴏɴᴜs 50+50", callback_data="ignore", style=style_50),
                InlineKeyboardButton(text="Bᴏɴᴜs 150+150", callback_data="ignore", style=style_150)
            ],
            [
                InlineKeyboardButton(text="Bᴏɴᴜs 250+250", callback_data="ignore", style=style_250),
                InlineKeyboardButton(text="Bᴏɴᴜs 500+500", callback_data="ignore", style=style_500)
            ]
        ])

        final_report = (
            f"<u><b>Mᴏʙɪʟᴇ Lᴇɢᴇɴᴅs Bᴀɴɢ Bᴀɴɢ</b></u>\n\n"
            f"🆔 <code>{'User ID' :<9}:</code> <code>{game_id}</code> (<code>{zone_id}</code>)\n"
            f"👤 <code>{'Nickname':<9}:</code> {html.escape(str(ig_name))}\n"
            f"🌍 <code>{'Region'  :<9}:</code> {html.escape(str(region))}\n"
            f"────────────────\n\n"
            f"🎁 <b>Fɪʀsᴛ Rᴇᴄʜᴀʀɢᴇ Bᴏɴᴜs Sᴛᴀᴛᴜs</b>{debug_bonus_error}"
        )

        await loading_msg.edit_text(final_report, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except Exception as e:
        await loading_msg.edit_text(f"❌ System Error: {html.escape(str(e))}", parse_mode=ParseMode.HTML)


@dp.message(F.text.regexp(r"(?i)^\.topup(?:\s+|$)"))
async def handle_topup_smart(message: types.Message):
    if not await is_authorized(message.from_user.id):
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")
    
    parts = message.text.strip().split()
    if len(parts) < 2:
        return await message.reply(" Usage: `.topup <code1> <code2> ...` (Up to 5 codes)")
        
    codes = parts[1:]
    if len(codes) > 5:
        return await message.reply("❌ တစ်ကြိမ်လျှင် Code အများဆုံး (၅) ခုသာ ထည့်သွင်းနိုင်ပါသည်။")
        
    tg_id = str(message.from_user.id)
    
    for code in codes:
        activation_code = code.strip()
        if not activation_code.isalnum():
            await message.reply(f"⚠️ Invalid code format: `{activation_code}`")
            continue
            
        created, existing = await db.create_topup_record(tg_id, activation_code, "AUTO")
        if not created:
            await message.reply(
                f"Code `{activation_code}` already submitted. "
                f"Status: `{(existing or {}).get('status', 'unknown')}`"
            )
            continue
            
        loading_msg = await message.reply(f"Code `{activation_code}` queued. Checking region automatically...")
        await TOPUP_QUEUE.put(TopupJob(tg_id, activation_code, "AUTO", loading_msg))



async def topup_worker():
    print("✅ Top-up worker started (single shared-account transaction lane).")
    while True:
        job = await TOPUP_QUEUE.get()
        try:
            if job.region == "BR":
                await process_topup_job_br(job)
            elif job.region == "PH":
                await process_topup_job_ph(job)
            elif job.region == "AUTO":
                await process_topup_job_auto(job)
            else:
                await db.update_topup_status(job.activation_code, "failed", error="Unsupported region")
                await job.loading_message.edit_text("❌ Unsupported top-up region.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"❌ Top-up worker error: {e}")
            try:
                await db.update_topup_status(job.activation_code, "failed", error=str(e)[:1000])
                await job.loading_message.edit_text(f"❌ Top-up system error: {html.escape(str(e))}")
            except Exception:
                pass
        finally:
            TOPUP_QUEUE.task_done()



async def process_topup_job_br(job: TopupJob):
    activation_code = job.activation_code
    tg_id = job.user_id
    loading_msg = job.loading_message
    await db.update_topup_status(activation_code, "processing")
    async with smile_account_lock:
        scraper = await get_main_scraper()
        
        page_url = 'https://www.smile.one/customer/activationcode'
        check_url = 'https://www.smile.one/smilecard/pay/checkcard'
        pay_url = 'https://www.smile.one/smilecard/pay/payajax'
        base_origin = 'https://www.smile.one'
        base_referer = 'https://www.smile.one/'
        balance_check_url = 'https://www.smile.one/customer/order'
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 
            'Accept': 'text/html',
            'Referer': base_referer
        }

        try:
            res = await scraper.get(page_url, headers=headers)
            
            if "login" in str(res.url).lower() or res.status_code in [403, 503]: 
                await loading_msg.edit_text("⚠️ <b>Cookies Expired!</b>\n\nAuto-login စတင်နေပါသည်... ခဏစောင့်ပြီး ပြန်လည်ကြိုးစားပါ။", parse_mode=ParseMode.HTML)
                await notify_owner("⚠️ <b>Top-up Alert (BR):</b> Cookie သက်တမ်းကုန်သွားပါသည်။ Auto-login စတင်နေပါသည်...")
                success = await auto_login_and_get_cookie()
                if not success: 
                    await notify_owner("❌ <b>Critical:</b> Auto-Login မအောင်မြင်ပါ။ `/setcookie` ဖြင့် အသစ်ထည့်ပေးပါ။")
                return

            soup = BeautifulSoup(res.text, 'html.parser')
            csrf_token = soup.find('meta', {'name': 'csrf-token'})
            
            if csrf_token:
                csrf_token = csrf_token.get('content')
            elif soup.find('input', {'name': '_csrf'}):
                csrf_token = soup.find('input', {'name': '_csrf'}).get('value')
            else:
                csrf_token = None
                
            if not csrf_token: 
                return await loading_msg.edit_text("❌ CSRF Token ရှာမတွေ့ပါ။ Cookie သက်တမ်းကုန်နေနိုင်ပါသည်။")

            ajax_headers = headers.copy()
            ajax_headers.update({
                'X-Requested-With': 'XMLHttpRequest', 
                'Origin': base_origin, 
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            })

            check_res_raw = await scraper.post(
                check_url, 
                data={'_csrf': csrf_token, 'pin': activation_code}, 
                headers=ajax_headers
            )
            check_res = check_res_raw.json()
            code_status = str(check_res.get('code', check_res.get('status', '')))
            
            card_amount = 0.0
            try:
                if 'data' in check_res and isinstance(check_res['data'], dict):
                    val = check_res['data'].get('amount', check_res['data'].get('money', 0))
                    if val: 
                        card_amount = float(val)
            except: 
                pass

            if code_status in ['200', '201', '0', '1'] or 'success' in str(check_res.get('msg', '')).lower():
                old_bal_value, old_bal_ok = await get_verified_smile_balance(
                    scraper, headers, balance_check_url, 'BR'
                )
                if not old_bal_ok:
                    await db.update_topup_status(activation_code, "failed", error="Unable to verify pre-payment balance/session")
                    return await loading_msg.edit_text("❌ Balance/session verification failed. Payment was not attempted.")
                old_bal = {'br_balance': old_bal_value}
                
                pay_res_raw = await scraper.post(
                    pay_url, 
                    data={'_csrf': csrf_token, 'sec': activation_code}, 
                    headers=ajax_headers
                )
                pay_res = pay_res_raw.json()
                pay_status = str(pay_res.get('code', pay_res.get('status', '')))
                
                if pay_status in ['200', '0', '1'] or 'success' in str(pay_res.get('msg', '')).lower():
                    await asyncio.sleep(3)
                    new_bal_value, new_bal_ok = await get_verified_smile_balance(
                        scraper, headers, balance_check_url, 'BR', retries=5
                    )
                    if not new_bal_ok:
                        await db.update_topup_status(activation_code, "uncertain", error="Payment response succeeded but post-payment balance could not be verified")
                        return await loading_msg.edit_text("⚠️ Payment response was received, but the new balance could not be verified. No success/amount was reported. Please verify the Smile.one balance before retrying.")
                    new_bal = {'br_balance': new_bal_value}
                    added_amount = round(new_bal_value - old_bal_value, 2)
                    if added_amount <= 0:
                        await db.update_topup_status(activation_code, "uncertain", error="Payment response succeeded but balance did not increase")
                        return await loading_msg.edit_text("⚠️ Payment response was received, but balance did not increase. No success/amount was reported. Please verify the Smile.one balance before retrying.")
                    if card_amount > 0 and added_amount + 0.01 < card_amount:
                        await db.update_topup_status(activation_code, "uncertain", error=f"Verified balance increase {added_amount} is below card amount {card_amount}")
                        return await loading_msg.edit_text("⚠️ Balance verification mismatch. No success/amount was reported. Please verify the Smile.one balance before retrying.")
                    
                    await db.update_topup_status(activation_code, "success", amount=added_amount)

                    # --- Fees တွက်ချက်ပြီး ဖြတ်တောက်မည့် Code အသစ် ---
                    if added_amount < 1000:
                        fee = 1.0
                    else:
                        # ၁၀၀၀ ပြည့်တိုင်း ၂ ကျပ်နှုန်း ဖြတ်ရန်
                        fee = float((added_amount // 1000) * 2)
                        
                    final_amount = added_amount - fee
                    
                    # User Wallet ထဲသို့ Fees နှုတ်ပြီးသား ပမာဏကိုသာ ထည့်ရန်
                    await db.change_user_balance(tg_id, final_amount) 
                    
                    fmt_amount = int(added_amount) if added_amount % 1 == 0 else added_amount
                    fmt_final_amount = int(final_amount) if final_amount % 1 == 0 else final_amount
                    assets = new_bal.get('br_balance', 0.0)
                    flag = f"<tg-emoji emoji-id='{BR_EMOJI}'>🇧🇷</tg-emoji>"
                        
                    msg = (
                        f"✅ <b>Code Top-Up Successful</b>\n\n"
                        f"<code>Code   : {activation_code} (BR)\n"
                        f"Amount : {fmt_amount:,} 🪙\n"
                        f"Fee    : -{fee} 🪙\n"
                        f"Added  : +{fmt_final_amount:,.1f} 🪙</code>\n"
                        f"<code>Total  : {assets:,.1f} 🪙</code>"
                    )
                    await loading_msg.edit_text(msg, parse_mode=ParseMode.HTML)
                else: 
                    await loading_msg.edit_text("❌ Payment failed during redemption.")
            else: 
                await loading_msg.edit_text("Cʜᴇᴄᴋ Fᴀɪʟᴇᴅ❌\n(Code is invalid or might have been used)")
                
        except Exception as e: 
            await loading_msg.edit_text(f"❌ Error: {str(e)}")


async def process_topup_job_ph(job: TopupJob):
    activation_code = job.activation_code
    tg_id = job.user_id
    loading_msg = job.loading_message
    await db.update_topup_status(activation_code, "processing")
    async with smile_account_lock:
        scraper = await get_main_scraper()
        
        page_url = 'https://www.smile.one/ph/customer/activationcode'
        check_url = 'https://www.smile.one/ph/smilecard/pay/checkcard'
        pay_url = 'https://www.smile.one/ph/smilecard/pay/payajax'
        base_origin = 'https://www.smile.one'
        base_referer = 'https://www.smile.one/ph/'
        balance_check_url = 'https://www.smile.one/ph/customer/order'
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 
            'Accept': 'text/html',
            'Referer': base_referer
        }

        try:
            res = await scraper.get(page_url, headers=headers)
            
            if "login" in str(res.url).lower() or res.status_code in [403, 503]: 
                await loading_msg.edit_text("⚠️ <b>Cookies Expired!</b>\n\nAuto-login စတင်နေပါသည်... ခဏစောင့်ပြီး ပြန်လည်ကြိုးစားပါ။", parse_mode=ParseMode.HTML)
                await notify_owner("⚠️ <b>Top-up Alert (PH):</b> Cookie သက်တမ်းကုန်သွားပါသည်။ Auto-login စတင်နေပါသည်...")
                success = await auto_login_and_get_cookie()
                if not success: 
                    await notify_owner("❌ <b>Critical:</b> Auto-Login မအောင်မြင်ပါ။ `/setcookie` ဖြင့် အသစ်ထည့်ပေးပါ။")
                return

            soup = BeautifulSoup(res.text, 'html.parser')
            csrf_token = soup.find('meta', {'name': 'csrf-token'})
            
            if csrf_token:
                csrf_token = csrf_token.get('content')
            elif soup.find('input', {'name': '_csrf'}):
                csrf_token = soup.find('input', {'name': '_csrf'}).get('value')
            else:
                csrf_token = None
                
            if not csrf_token: 
                return await loading_msg.edit_text("❌ CSRF Token ရှာမတွေ့ပါ။ Cookie သက်တမ်းကုန်နေနိုင်ပါသည်။")

            ajax_headers = headers.copy()
            ajax_headers.update({
                'X-Requested-With': 'XMLHttpRequest', 
                'Origin': base_origin, 
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            })

            check_res_raw = await scraper.post(
                check_url, 
                data={'_csrf': csrf_token, 'pin': activation_code}, 
                headers=ajax_headers
            )
            check_res = check_res_raw.json()
            code_status = str(check_res.get('code', check_res.get('status', '')))
            
            card_amount = 0.0
            try:
                if 'data' in check_res and isinstance(check_res['data'], dict):
                    val = check_res['data'].get('amount', check_res['data'].get('money', 0))
                    if val: 
                        card_amount = float(val)
            except: 
                pass

            if code_status in ['200', '201', '0', '1'] or 'success' in str(check_res.get('msg', '')).lower():
                old_bal_value, old_bal_ok = await get_verified_smile_balance(
                    scraper, headers, balance_check_url, 'PH'
                )
                if not old_bal_ok:
                    await db.update_topup_status(activation_code, "failed", error="Unable to verify pre-payment balance/session")
                    return await loading_msg.edit_text("❌ Balance/session verification failed. Payment was not attempted.")
                old_bal = {'ph_balance': old_bal_value}
                
                pay_res_raw = await scraper.post(
                    pay_url, 
                    data={'_csrf': csrf_token, 'sec': activation_code}, 
                    headers=ajax_headers
                )
                pay_res = pay_res_raw.json()
                pay_status = str(pay_res.get('code', pay_res.get('status', '')))
                
                if pay_status in ['200', '0', '1'] or 'success' in str(pay_res.get('msg', '')).lower():
                    await asyncio.sleep(3)
                    new_bal_value, new_bal_ok = await get_verified_smile_balance(
                        scraper, headers, balance_check_url, 'PH', retries=5
                    )
                    if not new_bal_ok:
                        await db.update_topup_status(activation_code, "uncertain", error="Payment response succeeded but post-payment balance could not be verified")
                        return await loading_msg.edit_text("⚠️ Payment response was received, but the new balance could not be verified. No success/amount was reported. Please verify the Smile.one balance before retrying.")
                    new_bal = {'ph_balance': new_bal_value}
                    added_amount = round(new_bal_value - old_bal_value, 2)
                    if added_amount <= 0:
                        await db.update_topup_status(activation_code, "uncertain", error="Payment response succeeded but balance did not increase")
                        return await loading_msg.edit_text("⚠️ Payment response was received, but balance did not increase. No success/amount was reported. Please verify the Smile.one balance before retrying.")
                    if card_amount > 0 and added_amount + 0.01 < card_amount:
                        await db.update_topup_status(activation_code, "uncertain", error=f"Verified balance increase {added_amount} is below card amount {card_amount}")
                        return await loading_msg.edit_text("⚠️ Balance verification mismatch. No success/amount was reported. Please verify the Smile.one balance before retrying.")
                    
                    await db.update_topup_status(activation_code, "success", amount=added_amount)
                    
                    # --- Fees တွက်ချက်ပြီး ဖြတ်တောက်မည့် Code အသစ် ---
                    if added_amount < 1000:
                        fee = 1.0
                    else:
                        # ၁၀၀၀ ပြည့်တိုင်း ၂ ကျပ်နှုန်း ဖြတ်ရန်
                        fee = float((added_amount // 1000) * 2)
                        
                    final_amount = added_amount - fee
                    
                    # User Wallet ထဲသို့ Fees နှုတ်ပြီးသား ပမာဏကိုသာ ထည့်ရန်
                    await db.change_user_balance(tg_id, final_amount) 
                    
                    fmt_amount = int(added_amount) if added_amount % 1 == 0 else added_amount
                    fmt_final_amount = int(final_amount) if final_amount % 1 == 0 else final_amount
                    assets = new_bal.get('ph_balance', 0.0)
                    flag = f"<tg-emoji emoji-id='{PH_EMOJI}'>🇵🇭</tg-emoji>"
                        
                    msg = (
                        f"✅ <b>Code Top-Up Successful</b>\n\n"
                        f"<code>Code   : {activation_code} (PH)\n"
                        f"Amount : {fmt_amount:,} 🪙\n"
                        f"Fee    : -{fee} 🪙\n"
                        f"Added  : +{fmt_final_amount:,.1f} 🪙</code>\n"
                        f"{flag} <code>Total  : {assets:,.1f} 🪙</code>"
                    )
                    await loading_msg.edit_text(msg, parse_mode=ParseMode.HTML)
                else: 
                    await loading_msg.edit_text("❌ Payment failed during redemption.")
            else: 
                await loading_msg.edit_text("Cʜᴇᴄᴋ Fᴀɪʟᴇᴅ❌\n(Code is invalid or might have been used)")
                
        except Exception as e: 
            await loading_msg.edit_text(f"❌ Error: {str(e)}")



async def process_topup_job_auto(job: TopupJob):
    activation_code = job.activation_code
    tg_id = job.user_id
    loading_msg = job.loading_message
    await db.update_topup_status(activation_code, "processing")
    
    async with smile_account_lock:
        scraper = await get_main_scraper()
        
        # BR ကို အရင်စစ်ဆေးပြီး မအောင်မြင်ပါက PH ကို စစ်ဆေးမည့် List
        regions_to_try = [
            {
                'name': 'BR',
                'page_url': 'https://www.smile.one/customer/activationcode',
                'check_url': 'https://www.smile.one/smilecard/pay/checkcard',
                'pay_url': 'https://www.smile.one/smilecard/pay/payajax',
                'base_referer': 'https://www.smile.one/',
                'balance_check_url': 'https://www.smile.one/customer/order',
                'emoji': BR_EMOJI
            },
            {
                'name': 'PH',
                'page_url': 'https://www.smile.one/ph/customer/activationcode',
                'check_url': 'https://www.smile.one/ph/smilecard/pay/checkcard',
                'pay_url': 'https://www.smile.one/ph/smilecard/pay/payajax',
                'base_referer': 'https://www.smile.one/ph/',
                'balance_check_url': 'https://www.smile.one/ph/customer/order',
                'emoji': PH_EMOJI
            }
        ]
        
        try:
            for region in regions_to_try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 
                    'Accept': 'text/html',
                    'Referer': region['base_referer']
                }
                
                res = await scraper.get(region['page_url'], headers=headers)
                
                if "login" in str(res.url).lower() or res.status_code in [403, 503]: 
                    await loading_msg.edit_text("⚠️ <b>Cookies Expired!</b>\n\nAuto-login စတင်နေပါသည်... ခဏစောင့်ပြီး ပြန်လည်ကြိုးစားပါ။", parse_mode=ParseMode.HTML)
                    success = await auto_login_and_get_cookie()
                    if not success: 
                        await notify_owner("❌ <b>Critical:</b> Auto-Login မအောင်မြင်ပါ။ `/setcookie` ဖြင့် အသစ်ထည့်ပေးပါ။")
                    return

                soup = BeautifulSoup(res.text, 'html.parser')
                csrf_token = soup.find('meta', {'name': 'csrf-token'})
                
                if csrf_token:
                    csrf_token = csrf_token.get('content')
                elif soup.find('input', {'name': '_csrf'}):
                    csrf_token = soup.find('input', {'name': '_csrf'}).get('value')
                else:
                    csrf_token = None
                    
                if not csrf_token: 
                    await loading_msg.edit_text("❌ CSRF Token ရှာမတွေ့ပါ။ Cookie သက်တမ်းကုန်နေနိုင်ပါသည်။")
                    return

                ajax_headers = headers.copy()
                ajax_headers.update({
                    'X-Requested-With': 'XMLHttpRequest', 
                    'Origin': 'https://www.smile.one', 
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                })

                check_res_raw = await scraper.post(
                    region['check_url'], 
                    data={'_csrf': csrf_token, 'pin': activation_code}, 
                    headers=ajax_headers
                )
                check_res = check_res_raw.json()
                code_status = str(check_res.get('code', check_res.get('status', '')))
                
                # Code မှန်ကန်ပါက ထို Region တွင် ငွေဖြည့်သွင်းမှုကို ဆက်လက်လုပ်ဆောင်မည်
                if code_status in ['200', '201', '0', '1'] or 'success' in str(check_res.get('msg', '')).lower():
                    card_amount = 0.0
                    try:
                        if 'data' in check_res and isinstance(check_res['data'], dict):
                            val = check_res['data'].get('amount', check_res['data'].get('money', 0))
                            if val: 
                                card_amount = float(val)
                    except: 
                        pass
                        
                    old_bal_value, old_bal_ok = await get_verified_smile_balance(
                        scraper, headers, region['balance_check_url'], region['name']
                    )
                    if not old_bal_ok:
                        await db.update_topup_status(activation_code, "failed", error="Unable to verify pre-payment balance")
                        return await loading_msg.edit_text("❌ Balance verification failed. Payment was not attempted.")
                    
                    pay_res_raw = await scraper.post(
                        region['pay_url'], 
                        data={'_csrf': csrf_token, 'sec': activation_code}, 
                        headers=ajax_headers
                    )
                    pay_res = pay_res_raw.json()
                    pay_status = str(pay_res.get('code', pay_res.get('status', '')))
                    
                    if pay_status in ['200', '0', '1'] or 'success' in str(pay_res.get('msg', '')).lower():
                        await asyncio.sleep(3)
                        new_bal_value, new_bal_ok = await get_verified_smile_balance(
                            scraper, headers, region['balance_check_url'], region['name'], retries=5
                        )
                        if not new_bal_ok:
                            return await loading_msg.edit_text("⚠️ Payment response was received, but new balance could not be verified.")
                        
                        added_amount = round(new_bal_value - old_bal_value, 2)
                        if added_amount <= 0:
                            return await loading_msg.edit_text("⚠️ Payment response was received, but balance did not increase.")
                        
                        await db.update_topup_status(activation_code, "success", amount=added_amount)

                        # --- Fees တွက်ချက်ခြင်း ---
                        if added_amount < 1000:
                            fee = 2.0
                        else:
                            fee = float((added_amount // 1000) * 2)
                            
                        final_amount = added_amount - fee
                        
                        await db.change_user_balance(tg_id, final_amount) 
                        
                        fmt_amount = int(added_amount) if added_amount % 1 == 0 else added_amount
                        fmt_final_amount = int(final_amount) if final_amount % 1 == 0 else final_amount
                        flag = f"<tg-emoji emoji-id='{region['emoji']}'>🇧🇷</tg-emoji>" if region['name'] == "BR" else f"<tg-emoji emoji-id='{region['emoji']}'>🇵🇭</tg-emoji>"
                            
                        msg = (
                            f"✅ <b>Code Top-Up Successful</b>\n\n"
                            f"<code>Code   : {activation_code} ({region['name']})\n"
                            f"Amount : {fmt_amount:,} 🪙\n"
                            f"Fee    : -{fee} 🪙\n"
                            f"Added  : +{fmt_final_amount:,.1f} 🪙</code>\n"
                            f"{flag} <code>Total  : {new_bal_value:,.1f} 🪙</code>"
                        )
                        await loading_msg.edit_text(msg, parse_mode=ParseMode.HTML)
                        return # Region တစ်ခုတွင် အောင်မြင်သွားပါက ကျန် Region များကို ထပ်မစစ်တော့ပါ။
                        
                    else: 
                        await loading_msg.edit_text(f"❌ Payment failed during redemption on {region['name']}.")
                        return
                
            # Region (၂) ခုလုံးတွင် Invalid ဖြစ်ခဲ့ပါက-
            await loading_msg.edit_text("Cʜᴇᴄᴋ Fᴀɪʟᴇᴅ❌\n(Code is invalid or might have been used)")
            
        except Exception as e: 
            await loading_msg.edit_text(f"❌ Error: {str(e)}")





@dp.message(F.text.regexp(r"(?i)^\.addbal\s+\d+\s+\d+(?:\.\d+)?$"))
async def add_balance_command(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("❌ Only the Owner can change user balance.")

    parts = message.text.split()
    target_id = parts[1].strip()
    amount = float(parts[2])
    if amount <= 0:
        return await message.reply("❌ Amount must be greater than 0.")

    ok, new_balance = await db.change_user_balance(target_id, amount)
    if not ok:
        if new_balance is None:
            return await message.reply(f"❌ User ID `{target_id}` is not authorized.")
        return await message.reply(f"❌ Balance update failed. Current balance: `{new_balance:,.2f}`")

    await message.reply(
        f"✅ Balance added\n\n👤 User ID: `{target_id}`\n"
        f"➕ Added: `{amount:,.2f}`\n💰 New Balance: `{new_balance:,.2f}`"
    )


@dp.message(F.text.regexp(r"(?i)^\.rmbal\s+\d+\s+\d+(?:\.\d+)?$"))
async def remove_balance_command(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply("❌ Only the Owner can change user balance.")

    parts = message.text.split()
    target_id = parts[1].strip()
    amount = float(parts[2])
    if amount <= 0:
        return await message.reply("❌ Amount must be greater than 0.")

    ok, new_balance = await db.change_user_balance(target_id, -amount)
    if not ok:
        if new_balance is None:
            return await message.reply(f"❌ User ID `{target_id}` is not authorized.")
        return await message.reply(
            f"❌ Insufficient balance.\n\n👤 User ID: `{target_id}`\n"
            f"💰 Current Balance: `{new_balance:,.2f}`"
        )

    await message.reply(
        f"✅ Balance removed\n\n👤 User ID: `{target_id}`\n"
        f"➖ Removed: `{amount:,.2f}`\n💰 New Balance: `{new_balance:,.2f}`"
    )


@dp.message(or_f(Command("add"), F.text.regexp(r"(?i)^\.add(?:$|\s+)")))
async def add_reseller(message: types.Message):
    if message.from_user.id != OWNER_ID: 
        return await message.reply("You are not the Owner.")
        
    parts = message.text.split()
    if len(parts) < 2: 
        return await message.reply("`/add <user_id>`")
        
    target_id = parts[1].strip()
    if not target_id.isdigit(): 
        return await message.reply("Please enter the User ID in numbers only.")
        
    if await db.add_reseller(target_id, f"User_{target_id}"): 
        await message.reply(f"✅ User ID `{target_id}` has been authorized to use the bot.")
    else: 
        await message.reply(f"User ID `{target_id}` is already in the list.")


@dp.message(or_f(Command("remove"), F.text.regexp(r"(?i)^\.remove(?:$|\s+)")))
async def remove_reseller_handler(message: types.Message):
    if message.from_user.id != OWNER_ID: 
        return await message.reply("You are not the Owner.")
        
    parts = message.text.split()
    if len(parts) < 2: 
        return await message.reply("Usage format - `/remove <user_id>`")
        
    target_id = parts[1].strip()
    if target_id == str(OWNER_ID): 
        return await message.reply("The Owner cannot be removed.")
        
    if await db.remove_reseller(target_id): 
        await message.reply(f"✅ User ID `{target_id}` has been removed.")
    else: 
        await message.reply("That ID is not in the list.")


@dp.message(or_f(Command("users"), F.text.regexp(r"(?i)^\.users$")))
async def list_resellers(message: types.Message):
    if message.from_user.id != OWNER_ID: 
        return await message.reply("You are not the Owner.")
        
    resellers_list = await db.get_all_resellers()
    user_list = []
    
    for r in resellers_list:
        role = "owner" if r["tg_id"] == str(OWNER_ID) else "authorized"
        balance = r.get("balance", 0.0)
        user_list.append(f"🟢 <code>{r['tg_id']}</code> | 🪙 <code>{balance:,.2f}</code> ({role})")
        
    final_text = "\n".join(user_list) if user_list else "No users found."
    await message.reply(f"🟢 **Authorized Users & Balances:**\n\n{final_text}", parse_mode=ParseMode.HTML)



@dp.message(Command("setcookie"))
async def set_cookie_command(message: types.Message):
    global GLOBAL_SCRAPER, GLOBAL_CSRF
    if message.from_user.id != OWNER_ID: 
        return await message.reply("❌ Only the Owner can set the Cookie.")
        
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: 
        return await message.reply("⚠️ **Usage format:**\n`/setcookie <Long_Main_Cookie>`")
        
    await db.update_main_cookie(parts[1].strip())
    GLOBAL_SCRAPER = None
    GLOBAL_CSRF = {'mlbb_br': None, 'mlbb_ph': None, 'mcc_br': None, 'mcc_ph': None}
    
    await message.reply("✅ **Main Cookie has been successfully updated securely.**")


@dp.message(or_f(Command("cookies"), F.text.regexp(r"(?i)^\.cookies$")))
async def check_cookie_status(message: types.Message):
    if message.from_user.id != OWNER_ID: 
        return await message.reply("❌ You are not authorized.")
        
    loading_msg = await message.reply("Checking Cookie status...")
    try:
        scraper = await get_main_scraper()
        headers = {
            'User-Agent': 'Mozilla/5.0', 
            'X-Requested-With': 'XMLHttpRequest', 
            'Origin': 'https://www.smile.one'
        }
        response = await scraper.get('https://www.smile.one/customer/order', headers=headers, timeout=15)
        
        if "login" not in str(response.url).lower() and response.status_code == 200: 
            await loading_msg.edit_text("🟢 Aᴄᴛɪᴠᴇ", parse_mode=ParseMode.HTML)
        else: 
            await loading_msg.edit_text("🔴 Exᴘɪʀᴇᴅ", parse_mode=ParseMode.HTML)
            
    except Exception as e: 
        await loading_msg.edit_text(f"❌ Error checking cookie: {str(e)}")


@dp.message(F.text.contains("PHPSESSID") & F.text.contains("cf_clearance"))
async def handle_smart_cookie_update(message: types.Message):
    global GLOBAL_SCRAPER, GLOBAL_CSRF
    if message.from_user.id != OWNER_ID: 
        return await message.reply("❌ You are not authorized.")
        
    text = message.text
    target_keys = ["PHPSESSID", "cf_clearance", "__cf_bm", "_did", "_csrf"]
    extracted_cookies = {}
    
    try:
        for key in target_keys:
            pattern = rf"['\"]?{key}['\"]?\s*[:=]\s*['\"]?([^'\",;\s}}]+)['\"]?"
            match = re.search(pattern, text)
            if match:
                extracted_cookies[key] = match.group(1)
                
        if "PHPSESSID" not in extracted_cookies or "cf_clearance" not in extracted_cookies:
            return await message.reply("❌ <b>Error:</b> `PHPSESSID` နှင့် `cf_clearance` ကို ရှာမတွေ့ပါ။ Format မှန်ကန်ကြောင်း စစ်ဆေးပါ။", parse_mode=ParseMode.HTML)
            
        formatted_cookie_str = "; ".join([f"{k}={v}" for k, v in extracted_cookies.items()])
        await db.update_main_cookie(formatted_cookie_str)
        
        GLOBAL_SCRAPER = None
        GLOBAL_CSRF = {'mlbb_br': None, 'mlbb_ph': None, 'mcc_br': None, 'mcc_ph': None}
        
        success_msg = "✅ <b>Cookies Successfully Extracted & Saved!</b>\n\n📦 <b>Extracted Data:</b>\n"
        for k, v in extracted_cookies.items():
            display_v = f"{v[:15]}...{v[-15:]}" if len(v) > 35 else v
            success_msg += f"🔸 <code>{k}</code> : {display_v}\n"
            
        success_msg += f"\n🍪 <b>Formatted Final String:</b>\n<code>{formatted_cookie_str}</code>"
        
        await message.reply(success_msg, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        await message.reply(f"❌ <b>Parsing Error:</b> {str(e)}", parse_mode=ParseMode.HTML)


@dp.message(or_f(Command("balance"), F.text.regexp(r"(?i)^\.bal(?:$|\s+)")))
async def check_balance_command(message: types.Message):
    tg_id = message.from_user.id

    if not await is_authorized(tg_id):
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")

    # OWNER: .bal = shared official Smile.one account balance.
    if tg_id == OWNER_ID:
        loading_msg = await message.reply("Fetching real balance from the official account...")
        scraper = await get_main_scraper()
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://www.smile.one'
        }
        try:
            anti_cache_url = f"https://www.smile.one/customer/order?_t={int(time.time())}"
            balances = await get_smile_balance(scraper, headers, anti_cache_url)
            br_flag = f"<tg-emoji emoji-id='{BR_EMOJI}'>🇧🇷</tg-emoji>"
            ph_flag = f"<tg-emoji emoji-id='{PH_EMOJI}'>🇵🇭</tg-emoji>"
            report = (
                f"<blockquote><b>𝗢𝗙𝗙𝗜𝗖𝗜𝗔𝗟 𝗔𝗖𝗖𝗢𝗨𝗡𝗧 𝗕𝗔𝗟𝗔𝗡𝗖𝗘</b>\n\n"
                f"{br_flag} <code>𝗕𝗥 𝗕𝗔𝗟𝗔𝗡𝗖𝗘 : ${balances.get('br_balance', 0.00):,.2f}</code>\n"
                f"{ph_flag} <code>𝗣𝗛 𝗕𝗔𝗟𝗔𝗡𝗖𝗘 : ${balances.get('ph_balance', 0.00):,.2f}</code></blockquote>"
            )
            await loading_msg.edit_text(report, parse_mode=ParseMode.HTML)
        except Exception as e:
            await loading_msg.edit_text(f"❌ Error fetching balance: {str(e)}")
        return

    # USER: .bal = this Telegram user's own wallet only.
    user_wallet_balance = await db.get_user_balance(tg_id)
    if user_wallet_balance is None:
        return await message.reply("❌ User wallet not found.")

    await message.reply(
        f"<blockquote><b>💰 𝗨𝗦𝗘𝗥 𝗪𝗔𝗟𝗟𝗘𝗧</b>\n\n"
        f"👤 <code>USER ID : {tg_id}</code>\n"
        f"💰 <code>𝗕𝗔𝗟𝗔𝗡𝗖𝗘 : {user_wallet_balance:,.2f} 🪙</code></blockquote>",
        parse_mode=ParseMode.HTML
    )


@dp.message(F.text.regexp(r"(?i)^(?:msc|mlb|br|b)\s+\d+"))
async def handle_br_mlbb(message: types.Message):
    if not await is_authorized(message.from_user.id): 
        return await message.reply(f"ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.❌")
        
    try:
        lines = [line.strip() for line in message.text.strip().split('\n') if line.strip()]
        regex = r"(?i)^(?:(?:b|br|mlb|msc)\s+)?(\d+)\s*\(?\s*(\d+)\s*\)?\s*(.+)$"
        
        total_pkgs = 0
        for line in lines:
            match = re.search(regex, line)
            if match:
                total_pkgs += len(match.group(3).split())
                
        if total_pkgs > 10: 
            return await message.reply("❌ 10 Limit Exceeded: တစ်ကြိမ်လျှင် အများဆုံး ၁၀ ခုသာ ဝယ်ယူနိုင်ပါသည်။")
            
        await execute_buy_process(message, lines, regex, 'BR', [DOUBLE_DIAMOND_PACKAGES, BR_PACKAGES], process_smile_one_order_br, "MLBB")
    except Exception as e: 
        await message.reply(f"System Error: {str(e)}")


@dp.message(F.text.regexp(r"(?i)^(?:mlp|ph|p)\s+\d+"))
async def handle_ph_mlbb(message: types.Message):
    if not await is_authorized(message.from_user.id): 
        return await message.reply(f"ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.❌")
        
    try:
        lines = [line.strip() for line in message.text.strip().split('\n') if line.strip()]
        regex = r"(?i)^(?:(?:p|ph|mlp|mcp)\s+)?(\d+)\s*\(?\s*(\d+)\s*\)?\s*(.+)$"
        
        total_pkgs = 0
        for line in lines:
            match = re.search(regex, line)
            if match:
                total_pkgs += len(match.group(3).split())
                
        if total_pkgs > 10: 
            return await message.reply("❌ 10 Limit Exceeded: တစ်ကြိမ်လျှင် အများဆုံး ၁၀ ခုသာ ဝယ်ယူနိုင်ပါသည်။")
            
        await execute_buy_process(message, lines, regex, 'PH', PH_PACKAGES, process_smile_one_order_ph, "MLBB")
    except Exception as e: 
        await message.reply(f"System Error: {str(e)}")


@dp.message(F.text.regexp(r"(?i)^(?:mcc|mcb)\s+\d+"))
async def handle_br_mcc(message: types.Message):
    if not await is_authorized(message.from_user.id): 
        return await message.reply(f"ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.❌")
        
    try:
        lines = [line.strip() for line in message.text.strip().split('\n') if line.strip()]
        regex = r"(?i)^(?:(?:mcc|mcb|mcp|mcgg)\s+)?(\d+)\s*\(?\s*(\d+)\s*\)?\s*(.+)$"
        
        total_pkgs = 0
        for line in lines:
            match = re.search(regex, line)
            if match:
                total_pkgs += len(match.group(3).split())
                
        if total_pkgs > 5: 
            return await message.reply("❌ 5 Limit Exceeded: တစ်ကြိမ်လျှင် အများဆုံး ၅ ခုသာ ဝယ်ယူနိုင်ပါသည်။")
            
        await execute_buy_process(message, lines, regex, 'BR', MCC_PACKAGES, process_mcc_order, "MCC", is_mcc=True)
    except Exception as e: 
        await message.reply(f"System Error: {str(e)}")


@dp.message(F.text.regexp(r"(?i)^mcp\s+\d+"))
async def handle_ph_mcc(message: types.Message):
    if not await is_authorized(message.from_user.id): 
        return await message.reply(f"ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.❌")
        
    try:
        lines = [line.strip() for line in message.text.strip().split('\n') if line.strip()]
        regex = r"(?i)^(?:mcp\s+)?(\d+)\s*\(?\s*(\d+)\s*\)?\s*(.+)$"
        
        total_pkgs = 0
        for line in lines:
            match = re.search(regex, line)
            if match:
                total_pkgs += len(match.group(3).split())
                
        if total_pkgs > 5: 
            return await message.reply("❌ 5 Limit Exceeded: တစ်ကြိမ်လျှင် အများဆုံး ၅ ခုသာ ဝယ်ယူနိုင်ပါသည်။")
            
        await execute_buy_process(message, lines, regex, 'PH', PH_MCC_PACKAGES, process_mcc_order, "MCC", is_mcc=True)
    except Exception as e: 
        await message.reply(f"System Error: {str(e)}")



@dp.message(or_f(Command("his"), F.text.regexp(r"(?i)^\.his(?:$|\s+)")))
async def check_history_txt_command(message: types.Message):
    tg_id = message.from_user.id

    if not await is_authorized(tg_id):
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")

    parts = message.text.split()
    target_id = str(tg_id)

    # စာသားတွင် User ID ပါ/မပါ စစ်ဆေးခြင်း
    if len(parts) > 1:
        if tg_id == OWNER_ID:
            target_id = parts[1].strip()
        else:
            return await message.reply("❌ အခြား User ၏ မှတ်တမ်းကို Owner သာ ကြည့်ရှုခွင့်ရှိပါသည်။")

    loading_msg = await message.reply(f"⏳ User ID `{target_id}` ၏ မှတ်တမ်းများကို txt ဖိုင်အဖြစ် ပြင်ဆင်နေပါသည်...")
    
    # နောက်ဆုံး ဝယ်ယူခဲ့သော အကြောင်းအရာ (၂၅၀) ခုကို ခေါ်ယူမည်
    history_records = await db.get_user_history(target_id, limit=250)
    
    if not history_records:
        return await loading_msg.edit_text(f"🤷‍♂️ User ID `{target_id}` တွင် ဝယ်ယူထားသော မှတ်တမ်း မရှိသေးပါ။")

    # Txt ဖိုင်အတွင်း ရေးသားမည့် စာသားများကို ပြင်ဆင်ခြင်း
    txt_content = "=========================================\n"
    txt_content += "           TRANSACTION HISTORY           \n"
    txt_content += "=========================================\n"
    txt_content += f"User ID: {target_id}\n"
    txt_content += f"Total Records: {len(history_records)} (Max 250)\n"
    txt_content += "-----------------------------------------\n\n"
    
    for idx, record in enumerate(history_records, 1):
        status_emoji = "✅ SUCCESS" if record.get('status') == 'success' else "❌ FAILED"
        game_id = record.get('game_id', 'Unknown')
        zone_id = record.get('zone_id', 'Unknown')
        item = record.get('item_name', 'Unknown')
        price = record.get('price', 0.0)
        date_str = record.get('date_str', '')
        order_id = record.get('order_id', 'N/A')

        txt_content += f"[{idx}] {status_emoji}\n"
        txt_content += f"UID    : {game_id} ({zone_id})\n"
        txt_content += f"Item   : {item}\n"
        txt_content += f"Price  : {price:,.2f} 🪙\n"
        txt_content += f"Date   : {date_str}\n"
        txt_content += f"Serial : {order_id}\n"
        txt_content += "-----------------------------------------\n"

    # စာသားများကို Bytes အဖြစ်ပြောင်း၍ BufferedInputFile ဖြင့် ဖိုင်တည်ဆောက်ခြင်း
    file_bytes = txt_content.encode('utf-8')
    txt_file = BufferedInputFile(file_bytes, filename=f"History_{target_id}.txt")
    
    # Document (txt) အဖြစ် ပေးပို့ခြင်း
    await message.reply_document(
        document=txt_file, 
        caption=f"📜 User ID: {target_id} ၏ နောက်ဆုံးဝယ်ယူခဲ့သော မှတ်တမ်း ({len(history_records)}) ခု"
    )
    await loading_msg.delete()



@dp.message(or_f(Command("rmhis"), F.text.regexp(r"(?i)^\.rmhis(?:$|\s+)")))
async def clear_history_command(message: types.Message):
    # Owner ဟုတ်/မဟုတ် စစ်ဆေးခြင်း
    if message.from_user.id != OWNER_ID:
        return await message.reply("❌ ဤ Command ကို Owner သာ အသုံးပြုနိုင်ပါသည်။")
        
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply("အသုံးပြုရမည့်ပုံစံ: `.rmhis <User_ID>`")
        
    target_id = parts[1].strip()
    
    # Database အတွင်းရှိ သတ်မှတ်ထားသော User ၏ မှတ်တမ်းများကို ရှင်းလင်းခြင်း
    deleted_count = await db.clear_user_history(target_id)
    
    if deleted_count > 0:
        await message.reply(f"✅ User ID `{target_id}` ၏ ဝယ်ယူမှုမှတ်တမ်းဟောင်း ({deleted_count}) ခုကို အောင်မြင်စွာ ရှင်းလင်းလိုက်ပါပြီ။")
    else:
        await message.reply(f"🤷‍♂️ User ID `{target_id}` တွင် ရှင်းလင်းရန် မှတ်တမ်း မရှိပါ။")







@dp.message(or_f(Command("maintenance"), F.text.regexp(r"(?i)^\.maintenance(?:$|\s+)")))
async def toggle_maintenance(message: types.Message):
    global IS_MAINTENANCE
    if message.from_user.id != OWNER_ID: 
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")
        
    parts = message.text.strip().lower().split()
    if len(parts) < 2 or parts[1] not in ["enable", "disable"]: 
        return await message.reply("⚠️ **Usage:** `.maintenance enable` သို့မဟုတ် `.maintenance disable`")
        
    if parts[1] == "enable":
        IS_MAINTENANCE = True
        await message.reply("✅ **Maintenance Mode ENABLED.**\nယခုအချိန်မှစ၍ Admin မှလွဲ၍ အခြား User များ Bot ကို အသုံးပြု၍ မရတော့ပါ။")
    else:
        IS_MAINTENANCE = False
        await message.reply("✅ **Maintenance Mode DISABLED.**\nBot ကို ပုံမှန်အတိုင်း ပြန်လည်အသုံးပြုနိုင်ပါပြီ။")


@dp.message(or_f(Command("scam"), F.text.regexp(r"(?i)^\.scam(?:$|\s+)")))
async def add_scam_id(message: types.Message):
    if not await is_authorized(message.from_user.id): 
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")
        
    parts = message.text.strip().split()
    if len(parts) < 2: 
        return await message.reply("⚠️ **Usage:** `.scam <Game_ID>`")
        
    scam_id = parts[1].strip()
    if not scam_id.isdigit(): 
        return await message.reply("❌ Invalid Game ID. ဂဏန်းများသာ ရိုက်ထည့်ပါ။")
        
    await db.add_scammer(scam_id)
    GLOBAL_SCAMMERS.add(scam_id)
    await message.reply(f"🚨 **Scammer ID Added:** <code>{scam_id}</code>\n✅ ဤ ID ကို Blacklist သို့ ထည့်သွင်းပြီးပါပြီ။", parse_mode=ParseMode.HTML)


@dp.message(or_f(Command("unscam"), F.text.regexp(r"(?i)^\.unscam(?:$|\s+)")))
async def remove_scam_id(message: types.Message):
    if not await is_authorized(message.from_user.id): 
        return await message.reply("ɴᴏᴛ ᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴜsᴇʀ.")
        
    parts = message.text.strip().split()
    if len(parts) < 2: 
        return await message.reply("⚠️ **Usage:** `.unscam <Game_ID>`")
        
    scam_id = parts[1].strip()
    removed = await db.remove_scammer(scam_id)
    GLOBAL_SCAMMERS.discard(scam_id)
    
    if removed: 
        await message.reply(f"✅ **Scammer ID Removed:** <code>{scam_id}</code>\nBlacklist ထဲမှ အောင်မြင်စွာ ဖယ်ရှားလိုက်ပါပြီ။", parse_mode=ParseMode.HTML)
    else: 
        await message.reply(f"⚠️ ထို ID သည် Scammer စာရင်းထဲတွင် မရှိပါ။")


@dp.message(or_f(Command("listmcgg"), F.text.regexp(r"(?i)^\.listmcgg$")))
async def handle_listmcgg(message: types.Message):
    pricelist = (
        "♟️ MAGIC CHESS GO GO PRICELIST ♟️\n"
        "=========================\n\n"

        "🇧🇷 Brazil (BR) Server\n"
        "-------------------------\n"
        "86             : 62.5 🪙\n"
        "172            : 125.0 🪙\n"
        "257            : 187.0 🪙\n"
        "343            : 250.0 🪙\n"
        "429            : 309.0 🪙\n"
        "516            : 375.0 🪙\n"
        "600            : 427.0 🪙\n"
        "706            : 500.0 🪙\n"
        "878            : 625.0 🪙\n"
        "963            : 687.0 🪙\n"
        "1049           : 749.5 🪙\n"
        "1135           : 812.0 🪙\n"
        "1412           : 1000.0 🪙\n"
        "1584           : 1105.0 🪙\n"
        "1755           : 1249.5 🪙\n"
        "2195           : 1500.0 🪙\n"
        "3688           : 2500.0 🪙\n"
        "5532           : 3750.0 🪙\n"
        "9288           : 6250.0 🪙\n"
        "b50            : 40.0 🪙 (50+50)\n"
        "b150           : 120.0 🪙 (150+150)\n"
        "b250           : 200.0 🪙 (250+250)\n"
        "b500           : 400.0 🪙 (500+500)\n"
        "wp             : 99.9 🪙\n\n"

        "🇵🇭 Philippines (PH) Server\n"
        "-------------------------\n"
        "5              : 4.75 🪙\n"
        "11             : 9.03 🪙\n"
        "22             : 18.05 🪙\n"
        "56             : 45.13 🪙\n"
        "112            : 90.25 🪙\n"
        "223            : 180.50 🪙\n"
        "339            : 270.75 🪙\n"
        "570            : 451.25 🪙\n"
        "1163           : 902.50 🪙\n"
        "2398           : 1805.0 🪙\n"
        "6042           : 4512.5 🪙\n"
        "wp             : 95.0 🪙\n"
        "lukas          : 47.45 🪙\n"
        "battlefordis.  : 47.45 🪙\n"
        "========================="
    )

    await message.reply(f"<code>{pricelist}</code>", parse_mode=ParseMode.HTML)


@dp.message(or_f(Command("listmlbb"), F.text.regexp(r"(?i)^\.listmlbb$")))
async def handle_listmlbb(message: types.Message):
    pricelist = (
        "💎 MOBILE LEGENDS PRICELIST 💎\n"
        "=========================\n\n"

        "🇧🇷 Brazil (BR) Server\n"
        "-------------------------\n"
        "86             : 61.5 🪙\n"
        "172            : 122.0 🪙\n"
        "257            : 177.5 🪙\n"
        "343            : 239.0 🪙\n"
        "429            : 299.5 🪙\n"
        "514            : 355.0 🪙\n"
        "600            : 416.5 🪙\n"
        "706            : 480.0 🪙\n"
        "878            : 602.0 🪙\n"
        "963            : 657.5 🪙\n"
        "1049           : 719.0 🪙\n"
        "1135           : 779.5 🪙\n"
        "1412           : 960.0 🪙\n"
        "1584           : 1082.0 🪙\n"
        "1755           : 1199.0 🪙\n"
        "2195           : 1453.0 🪙\n"
        "2538           : 1692.0 🪙\n"
        "2901           : 1933.0 🪙\n"
        "3244           : 2172.0 🪙\n"
        "3688           : 2424.0 🪙\n"
        "5532           : 3660.0 🪙\n"
        "9288           : 6079.0 🪙\n"
        "meb            : 196.5 🪙\n"
        "tp             : 402.5 🪙\n"
        "web            : 39.0 🪙\n"
        "B50            : 39.0 🪙 (50+50)\n"
        "B150           : 116.9 🪙 (150+150)\n"
        "B250           : 187.5 🪙 (250+250)\n"
        "B500           : 385.0 🪙 (500+500)\n"
        "wp             : 76.0 🪙 (wp1-10)\n\n"

        "🇵🇭 Philippines (PH) Server\n"
        "-------------------------\n"
        "11             : 9.5 🪙\n"
        "22             : 19.0 🪙\n"
        "33             : 28.5 🪙\n"
        "44             : 38.0 🪙\n"
        "56             : 47.5 🪙\n"
        "112            : 95.0 🪙\n"
        "223            : 190.0 🪙\n"
        "336            : 285.0 🪙\n"
        "570            : 475.0 🪙\n"
        "1163           : 950.0 🪙\n"
        "2398           : 1900.0 🪙\n"
        "6042           : 4750.0 🪙\n"
        "tp             : 475.0 🪙\n"
        "wp             : 95.0 🪙 (wp1-10)\n"
        "========================="
    )

    await message.reply(f"<code>{pricelist}</code>", parse_mode=ParseMode.HTML)

@dp.message(or_f(Command("help"), F.text.regexp(r"(?i)^\.help$")))
async def send_help_message(message: types.Message):
    is_owner = (message.from_user.id == OWNER_ID)
    
    help_text = (
        f"<blockquote><b>🤖 Sajii Dia BOT COMMANDS</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 <b>USER COMMANDS</b>\n\n"
        f"💎 <b>MLBB Order</b>\n"
        f"B [ID] [Zone] [Amt] — BR Server\n"
        f"P [ID] [Zone] [Amt] — PH Server\n\n"
        f"🎮 <b>Magic Chess Order</b>\n"
        f"<code>.mcb</code> [ID] [Zone] [Amt] — BR\n"
        f"<code>.mcp</code> [ID] [Zone] [Amt] — PH\n\n"
        f"🪙 <b>Smile Code Topup</b>\n"
        f"<code>.topup</code> [Code] B\n"
        f"<code>.topup</code> [Code] P\n\n"
        f"🛠️ <b>Tools</b>\n"
        f"<code>.role</code> — [ID] [Zone] Region စစ်ရန်\n"
        f"<code>.bal</code> — Coin Balance\n"
        f"<code>.his</code> — မှတ်တမ်းကြည့်ရန်\n"
        f"<code>.listmlbb</code> — MLBB ဈေးနှုန်း\n"
        f"<code>.listmcgg</code> — MCGG ဈေးနှုန်း\n"
    )
    
    if is_owner:
        help_text += (
            f"\n━━━━━━━━━━━━━━━━━\n"
            f"<b>👑 𝐎𝐰𝐧𝐞𝐫 𝐓𝐨𝐨ls (Admin သီးသန့်)</b>\n\n"
            f"🔸 <code>.add ID</code> : အသုံးပြုခွင့်ပေးရန်\n"
            f"🔸 <code>.remove ID</code> : အသုံးပြုခွင့်ပိတ်ရန်\n"
            f"🔸 <code>.users</code> : User စာရင်းအားလုံး ကြည့်ရန်\n"
        )
        
    help_text += f"</blockquote>"
    await message.reply(help_text, parse_mode=ParseMode.HTML)



@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    tg_id = str(message.from_user.id)
    full_name = "User"
    try:
        first_name = message.from_user.first_name or ""
        last_name = message.from_user.last_name or ""
        full_name = f"{first_name} {last_name}".strip() or "User"
        safe_full_name = full_name.replace('<', '').replace('>', '')
        username_display = f'<a href="tg://user?id={tg_id}">{safe_full_name}</a>'
        
        EMOJI_1, EMOJI_2, EMOJI_3, EMOJI_4, EMOJI_5 = "5956355397366320202", "5954097490109140119", "5958289678837746828", "5956330306167376831", "5954078884310814346"

        status = "🟢 Aᴄᴛɪᴠᴇ" if await is_authorized(message.from_user.id) else "🔴 Nᴏᴛ Aᴄᴛɪᴠᴇ"
        
        welcome_text = (
            f"ʜᴇʏ ʙᴀʙʏ <tg-emoji emoji-id='{EMOJI_1}'>🥺</tg-emoji>\n\n"
            f"<tg-emoji emoji-id='{EMOJI_2}'>👤</tg-emoji> {'Usᴇʀɴᴀᴍᴇ' :<11}: {username_display}\n"
            f"<tg-emoji emoji-id='{EMOJI_3}'>🆔</tg-emoji> {'𝐈𝐃' :<11}: <code>{tg_id}</code>\n"
            f"<tg-emoji emoji-id='{EMOJI_4}'>📊</tg-emoji> {'Sᴛᴀᴛᴜs' :<11}: {status}\n\n"
            f"<tg-emoji emoji-id='{EMOJI_5}'>📞</tg-emoji> {'Cᴏɴᴛᴀᴄᴛ ᴜs' :<11}: @iwillgoforwardsalone"
        )
        await message.reply(welcome_text, parse_mode=ParseMode.HTML)
        
    except Exception:
        fallback_text = (
            f"ʜᴇʏ ʙᴀʙʏ 🥺\n\n"
            f"👤 {'Usᴇʀɴᴀᴍᴇ' :<11}: {full_name}\n"
            f"🆔 {'𝐈𝐃' :<11}: <code>{tg_id}</code>\n"
            f"📊 {'Sᴛᴀᴛᴜs' :<11}: 🔴 Nᴏᴛ Aᴄᴛɪᴠᴇ\n\n"
            f"📞 {'Cᴏɴᴛᴀᴄᴛ ᴜs' :<11}: @iwillgoforwardsalone"
        )
        await message.reply(fallback_text, parse_mode=ParseMode.HTML)

# ==========================================
# 6. Middlewares & Scheduled Tasks
# ==========================================
class MaintenanceMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data: dict):
        if IS_MAINTENANCE and event.from_user.id != OWNER_ID:
            await event.reply("ပြုပြင်ဆောင်ရွက်မှုများလုပ်နေပါသဖြင့် Topup ဘော့အား ခနရပ်ထားပါသည်။")
            return
        return await handler(event, data)


class ScamAlertMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: types.Message, data: dict):
        if event.text:
            text_lower = event.text.lower()
            if text_lower.startswith((".scam ", ".unscam ", "/scam", "/unscam")):
                return await handler(event, data)
                
            for scam_id in GLOBAL_SCAMMERS:
                if re.search(rf"\b{scam_id}\b", event.text):
                    await event.reply("Scamer game id , Scamer Alert!", parse_mode=ParseMode.HTML)
                    break 
                    
        return await handler(event, data)


async def keep_cookie_alive():
    while True:
        try:
            await asyncio.sleep(2 * 60) 
            scraper = await get_main_scraper()
            headers = {
                'User-Agent': 'Mozilla/5.0', 
                'X-Requested-With': 'XMLHttpRequest', 
                'Origin': 'https://www.smile.one'
            }
            
            response = await scraper.get('https://www.smile.one/customer/order', headers=headers)
            
            if "login" not in str(response.url).lower() and response.status_code == 200:
                pass 
            else:
                print(f"[{datetime.datetime.now(MMT).strftime('%I:%M %p')}] ⚠️ Main Cookie expired unexpectedly.")
                await notify_owner("⚠️ <b>System Warning:</b> Cookie သက်တမ်းကုန်သွားသည်ကို တွေ့ရှိရပါသည်။ Auto-Login စတင်နေပါသည်...")
                success = await auto_login_and_get_cookie()
                
                if not success: 
                    await notify_owner("❌ <b>Critical:</b> Auto-Login မအောင်မြင်ပါ။ သင့်အနေဖြင့် `/setcookie` ဖြင့် Cookie အသစ် လာရောက်ထည့်သွင်းပေးရန် လိုအပ်ပါသည်။")
        except Exception: 
            pass


async def send_broadcast_greeting(text: str):
    users = await db.get_all_resellers()
    for u in users:
        try:
            await bot.send_message(chat_id=int(u['tg_id']), text=text, parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.1) 
        except Exception: 
            pass

# ==========================================
# 7. Main Execution Flow
# ==========================================
async def main():
    print("Starting Heartbeat & Auto-login tasks...")
    print("နှလုံးသားမပါရင် ဘယ်အရာမှတရားမဝင်")
    
    loop = asyncio.get_running_loop()
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=50))
    
    try:
        scammer_list = await db.get_all_scammers()
        global GLOBAL_SCAMMERS
        GLOBAL_SCAMMERS = set(scammer_list)
        print(f"Loaded {len(GLOBAL_SCAMMERS)} Scammer IDs.")
    except Exception as e:
        print(f"Error loading scammers: {e}")

    dp.message.middleware(MaintenanceMiddleware())
    dp.message.middleware(ScamAlertMiddleware())
    
    asyncio.create_task(keep_cookie_alive())
    global TOPUP_WORKER_TASK
    TOPUP_WORKER_TASK = asyncio.create_task(topup_worker())
    
    await db.setup_indexes()
    await db.init_owner(OWNER_ID)
    
    print("Bot is successfully running on Aiogram 3 Framework... 🎉")
    
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
