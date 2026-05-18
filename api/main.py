from fastapi import (
    FastAPI, HTTPException, Depends, Query, Path,
    WebSocket, WebSocketDisconnect
)
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
import asyncio, json, httpx, os, re, time
import xml.etree.ElementTree as ET
import pandas as pd
import pandas_ta_classic as ta  # type: ignore
import jwt
import feedparser
import secrets
import string
import firebase_admin
from firebase_admin import credentials, auth as firebase_auth
from deep_translator import GoogleTranslator
from dotenv import load_dotenv
from database import get_db_connection, async_redis_client
from workers import start
from sqlalchemy import create_engine as _create_engine
from google.oauth2 import id_token
from google.auth.transport import requests

def _get_analysis_engine():
    import os
    h = os.getenv("DB_HOST","localhost")
    p = os.getenv("DB_PORT","4000")
    u = os.getenv("DB_USER","root")
    pw = os.getenv("DB_PASSWORD","")
    db = os.getenv("DB_NAME","cryptopulse")
    return _create_engine(
        f"mysql+pymysql://{u}:{pw}@{h}:{p}/{db}"
        f"?ssl_verify_cert=false&ssl_verify_identity=false",
        pool_size=2, pool_recycle=1800
    )

_analysis_engine = _get_analysis_engine()

# ══════════════════════════════════════════════════════════════════
#  1. CẤU HÌNH
# ══════════════════════════════════════════════════════════════════
load_dotenv()

MORALIS_API_KEY          = os.getenv("MORALIS_API_KEY", "")
MORALIS_BASE             = "https://deep-index.moralis.io/api/v2.2"
ZERO_ADDRESS             = "0x0000000000000000000000000000000000000000"
SECRET_KEY               = os.getenv("SECRET_KEY", "CryptoPulse_Super_Secret_Key_2026")
ALGORITHM                = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30

# BCrypt context — hash mật khẩu an toàn
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ══════════════════════════════════════════════════════════════════
#  2. WHALE NEWS — CONSTANTS
# ══════════════════════════════════════════════════════════════════
WHALE_KEYWORDS = [
    "bought","purchased","acquired","adds","buys","buy",
    "treasury","reserve","hodl","accumulate","accumulation",
    "invest","investment","holding","holds","stake",
    "institutional","strategy","adoption","etf","fund",
    "microstrategy","blackrock","fidelity","grayscale","ark invest",
    "coinbase","binance","kraken","bitfinex",
    "mua","tích lũy","dự trữ","nắm giữ","đầu tư",
    "quỹ","tổ chức","chiến lược",
]

TRACKED_COINS = [
    "BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","DOT","LINK",
    "bitcoin","ethereum","binance","solana","ripple","cardano",
    "dogecoin","avalanche","polkadot","chainlink",
]

DEFAULT_COIN_IMAGES = {
    "BTC":  "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
    "ETH":  "https://assets.coingecko.com/coins/images/279/large/ethereum.png",
    "BNB":  "https://assets.coingecko.com/coins/images/825/large/bnb-icon2_2x.png",
    "SOL":  "https://assets.coingecko.com/coins/images/4128/large/solana.png",
    "XRP":  "https://assets.coingecko.com/coins/images/44/large/xrp-symbol-white-128.png",
    "ADA":  "https://assets.coingecko.com/coins/images/975/large/cardano.png",
    "DOGE": "https://assets.coingecko.com/coins/images/5/large/dogecoin.png",
    "AVAX": "https://assets.coingecko.com/coins/images/12559/large/Avalanche_Circle_RedWhite_Trans.png",
    "DOT":  "https://assets.coingecko.com/coins/images/12171/large/polkadot.png",
    "LINK": "https://assets.coingecko.com/coins/images/877/large/chainlink-new-logo.png",
}

# ══════════════════════════════════════════════════════════════════
#  3. AUTH HELPERS
# ══════════════════════════════════════════════════════════════════
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    try:
        if pwd_context.verify(plain, hashed):
            return True
    except Exception:
        pass
    import hashlib
    return hashlib.sha256(plain.encode()).hexdigest() == hashed

def create_token(data: dict, days: int = ACCESS_TOKEN_EXPIRE_DAYS) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=days)
    return jwt.encode({**data, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)

# ══════════════════════════════════════════════════════════════════
#  4. TRANSLATION HELPERS
# ══════════════════════════════════════════════════════════════════
def _translate_vi(text: str, max_len: int = 500) -> str:
    if not text or not text.strip():
        return text
    try:
        return GoogleTranslator(source="auto", target="vi").translate(text[:max_len])
    except Exception as e:
        print(f"[Translate] Lỗi: {e}")
        return text

def _translate_batch(items: list) -> list:
    translated = []
    for i, item in enumerate(items):
        title_en   = item.get("title",   "") or ""
        summary_en = item.get("summary", "") or ""
        translated.append({
            **item,
            "title":            _translate_vi(title_en,   200),
            "title_original":   title_en,
            "summary":          _translate_vi(summary_en, 400),
            "summary_original": summary_en,
            "language":         "vi",
        })
        if (i + 1) % 5 == 0:
            time.sleep(0.5)
    return translated

# ══════════════════════════════════════════════════════════════════
#  5. WHALE NEWS — FETCH HELPERS
# ══════════════════════════════════════════════════════════════════
def _is_whale_news(title: str, body: str = "") -> bool:
    text = (title + " " + body).lower()
    return (any(kw.lower() in text for kw in WHALE_KEYWORDS) and
            any(c.lower() in text for c in TRACKED_COINS))

def _extract_coins_from_text(text: str) -> list:
    COIN_MAP = {
        "bitcoin":"BTC","btc":"BTC","ethereum":"ETH","eth":"ETH",
        "binance":"BNB","bnb":"BNB","solana":"SOL","sol":"SOL",
        "ripple":"XRP","xrp":"XRP","cardano":"ADA","ada":"ADA",
        "dogecoin":"DOGE","doge":"DOGE","avalanche":"AVAX","avax":"AVAX",
        "polkadot":"DOT","dot":"DOT","chainlink":"LINK","link":"LINK",
        "polygon":"POL","matic":"POL","pol":"POL",
    }
    text_lower = text.lower()
    seen, result = set(), []
    for keyword, symbol in COIN_MAP.items():
        if keyword in text_lower and symbol not in seen:
            seen.add(symbol); result.append(symbol)
    return result[:5]

async def _fetch_cryptocompare() -> list:
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            res = await client.get(
                "https://min-api.cryptocompare.com/data/v2/news/",
                params={"lang": "EN", "sortOrder": "latest"}
            )
        
        if res.status_code != 200: return []
        news_json = res.json()
        news_data = news_json.get("Data") or []

        if isinstance(news_data, dict):
            news_data = list(news_data.values())
        
        result = []
        for item in news_data[:20]:
            title = item.get("title", "")
            body = item.get("body", "")
            
            if not _is_whale_news(title, body):
                continue
                
            result.append({
                "title":     title,
                "summary":   re.sub(r"<[^>]+>", "", body)[:400],
                "url":       item.get("url", ""),
                "source":    item.get("source", "CryptoCompare"),
                "image":     item.get("imageurl", ""),
                "published": datetime.fromtimestamp(
                                item.get("published_on", 0), 
                                tz=timezone.utc
                             ).isoformat(),
                "coins":     _extract_coins_from_text(title + " " + body),
            })
        return result
    except Exception as e:
        print(f"[CC] Lỗi hệ thống: {e}")
        return []

async def _fetch_coindesk() -> list:
    try:
        async with httpx.AsyncClient(timeout=12,headers={"User-Agent":"Mozilla/5.0"}) as client:
            res = await client.get("https://www.coindesk.com/arc/outboundfeeds/rss/")
        if res.status_code != 200: return []
        feed = feedparser.parse(res.text)
        result = []
        for entry in feed.entries[:15]:
            title = entry.get("title",""); desc = entry.get("summary","")
            if not _is_whale_news(title,desc): continue
            image = ""
            for src in [
                lambda: (entry.get("media_content") or [{}])[0].get("url",""),
                lambda: (entry.get("media_thumbnail") or [{}])[0].get("url",""),
                lambda: (entry.enclosures[0].get("href","") if getattr(entry,"enclosures",None) else ""),
                lambda: (re.search(r'<img[^>]+src=["\']([^"\']+)["\']',desc) or type("",(),{"group":lambda s,i:""})()).group(1),
            ]:
                try:
                    image = src()
                    if image: break
                except Exception: pass
            result.append({
                "title":title,"summary":re.sub(r"<[^>]+>","",desc)[:400],
                "url":entry.get("link",""),"source":"CoinDesk","image":image,
                "published":entry.get("published",""),
                "coins":_extract_coins_from_text(title+" "+desc),
            })
        return result
    except Exception as e:
        print(f"[CoinDesk] Lỗi: {e}"); return []

async def _fetch_cointelegraph() -> list:
    try:
        async with httpx.AsyncClient(timeout=12,headers={"User-Agent":"Mozilla/5.0"}) as client:
            res = await client.get("https://cointelegraph.com/rss")
        if res.status_code != 200: return []
        ns = {"media":"http://search.yahoo.com/mrss/"}
        root = ET.fromstring(res.text)
        result = []
        for item in root.findall(".//item"):
            title = item.findtext("title",""); desc = item.findtext("description","")
            if not _is_whale_news(title,desc): continue
            image = ""
            for tag in ["media:content","media:thumbnail"]:
                el = item.find(tag,ns)
                if el is not None:
                    image = el.get("url","")
                    if image: break
            if not image:
                enc = item.find("enclosure")
                if enc is not None: image = enc.get("url","")
            if not image:
                m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']',desc)
                if m: image = m.group(1)
            result.append({
                "title":title,"summary":re.sub(r"<[^>]+>","",desc)[:400],
                "url":item.findtext("link",""),"source":"CoinTelegraph","image":image,
                "published":item.findtext("pubDate",""),
                "coins":_extract_coins_from_text(title+" "+desc),
            })
        return result
    except Exception as e:
        print(f"[CT] Lỗi: {e}"); return []

async def _fetch_cryptopanic() -> list:
    token = os.getenv("CRYPTOPANIC_TOKEN","")
    plan  = os.getenv("CRYPTOPANIC_PLAN","developer")
    if not token: return []
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.get(
                f"https://cryptopanic.com/api/{plan}/v2/posts/"
                f"?auth_token={token}&public=true&kind=news&filter=hot"
                f"&currencies=BTC,ETH,BNB,SOL,XRP,ADA,DOGE,AVAX,DOT,LINK&regions=en"
            )
        if resp.status_code not in (200,): return []
        panic_json = resp.json()
        panic_data = panic_json.get("results") or []
        result = []
        for item in resp.json().get("results",[]):
            title = item.get("title",""); desc = item.get("description","") or ""
            if not _is_whale_news(title,desc): continue
            instruments = item.get("instruments") or []
            coins = ([i["code"] for i in instruments if i.get("code")]
                     or _extract_coins_from_text(title+" "+desc))
            votes = item.get("votes") or {}
            result.append({
                "title":title,"summary":desc[:400]+("..." if len(desc)>400 else ""),
                "url":item.get("original_url") or item.get("url"),
                "source":(item.get("source") or {}).get("title","CryptoPanic"),
                "image":item.get("image",""),"published":item.get("published_at",""),
                "coins":coins,
                "votes":{"positive":votes.get("positive",0),"negative":votes.get("negative",0),
                         "important":votes.get("important",0)},
                "panic_score":item.get("panic_score"),
            })
        return result
    except Exception as e:
        print(f"[CryptoPanic] Lỗi: {e}"); return []

# ══════════════════════════════════════════════════════════════════
#  6. BUILD WHALE PAYLOAD
# ══════════════════════════════════════════════════════════════════
async def _build_whale_news_payload() -> dict:
    cc,cd,ct,cp = await asyncio.gather(
        _fetch_cryptocompare(), _fetch_coindesk(),
        _fetch_cointelegraph(), _fetch_cryptopanic(),
        return_exceptions=True
    )
    sources = []
    for s in [cc, cd, ct, cp]:
        if isinstance(s, Exception):
            print(f"[Whale] Source lỗi: {s}")
            continue
        sources += s

    all_news = sources

    seen, unique = set(), []
    for n in all_news:
        key = n["title"].lower()[:60]
        if key not in seen:
            seen.add(key); unique.append(n)

    unique.sort(key=lambda x: x.get("published",""), reverse=True)
    top40 = unique[:40]

    print(f"[Whale] Đang dịch {len(top40)} bài...")
    translated = await asyncio.get_event_loop().run_in_executor(None, _translate_batch, top40)

    for item in translated:
        if not item.get("image"):
            coins = item.get("coins",[])
            if coins: item["image"] = DEFAULT_COIN_IMAGES.get(coins[0],"")

    print(f"[Whale] Dịch xong {len(translated)} bài.")

    return {
        "status":"success",
        "count":len(translated),
        "updated_at":datetime.now(timezone.utc).isoformat(),
        "data":translated
    }
# ══════════════════════════════════════════════════════════════════
#  7. BACKGROUND WORKERS
# ══════════════════════════════════════════════════════════════════
async def whale_news_worker():
    while True:
        try:
            print("[Worker] Fetch & dịch Whale News...")
            payload = await asyncio.wait_for(_build_whale_news_payload(), timeout=300)
            await async_redis_client.setex(
                "whale_institutional_news", 900,
                json.dumps(payload, ensure_ascii=False))
            print(f"[Worker] ✓ {payload['count']} bài cached.")
        except asyncio.TimeoutError:
            print("[Worker] ⚠ Timeout.")
        except Exception as e:
            print(f"[Worker] ❌ {e}")
        await asyncio.sleep(900)

async def crawl_worker():
    while True:
        try:
            async with httpx.AsyncClient(timeout=15,headers={"User-Agent":"Mozilla/5.0"}) as client:
                res = await client.get("https://www.coindesk.com/arc/outboundfeeds/rss/")
            if res.status_code == 200:
                feed = feedparser.parse(res.text)
                conn = cursor = None
                try:
                    conn = get_db_connection(); cursor = conn.cursor(dictionary=True)
                    inserted = 0
                    for entry in feed.entries[:10]:
                        url = entry.get("link","")
                        if not url: continue
                        cursor.execute("SELECT id FROM news WHERE original_url=%s",(url,))
                        if cursor.fetchone(): continue
                        title_vi   = await asyncio.get_event_loop().run_in_executor(
                            None, lambda t=entry.get("title",""):   _translate_vi(t,200))
                        content_vi = await asyncio.get_event_loop().run_in_executor(
                            None, lambda c=entry.get("summary",""): _translate_vi(c,500))
                        image_url = ""
                        media = entry.get("media_content",[])
                        if media: image_url = media[0].get("url","")
                        if not image_url:
                            thumb = entry.get("media_thumbnail",[])
                            if thumb: image_url = thumb[0].get("url","")
                        cursor.execute(
                            "INSERT INTO news(title,content,image_url,original_url,author,status,category_id)"
                            " VALUES(%s,%s,%s,%s,%s,'published',3)",
                            (title_vi,content_vi,image_url,url,"Bot (CoinDesk)"))
                        inserted += 1
                        await asyncio.sleep(0.5)
                    conn.commit()
                    print(f"[Crawl] ✓ {inserted} bài mới.")
                except Exception as e:
                    if conn: conn.rollback(); print(f"[Crawl] ❌ {e}")
                finally:
                    if cursor: cursor.close()
                    if conn:   conn.close()
        except Exception as e:
            print(f"[Crawl] ❌ {e}")
        await asyncio.sleep(1800)

# ══════════════════════════════════════════════════════════════════
#  8. LIFESPAN
# ══════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    t1 = asyncio.create_task(start())
    t2 = asyncio.create_task(whale_news_worker())
    t3 = asyncio.create_task(crawl_worker())
    
    yield
    
    for t in (t1, t2, t3):
        t.cancel()
    
    await asyncio.gather(t1, t2, t3, return_exceptions=True)  

# ══════════════════════════════════════════════════════════════════
#  9. APP INIT
# ══════════════════════════════════════════════════════════════════
app = FastAPI(
    title="CryptoPulse API Gateway",
    version="3.2",
    description="Real-time crypto · Analysis · On-chain · News · Auth",
    lifespan=lifespan,
)

import base64, json as _json

# Khởi tạo Firebase an toàn
firebase_key_b64 = os.getenv("FIREBASE_SERVICE_ACCOUNT_B64", "")
try:
    if firebase_key_b64:
        key_dict = _json.loads(base64.b64decode(firebase_key_b64).decode())
        cred = credentials.Certificate(key_dict)
    else:
        # fallback cho local dev
        key_path = "serviceAccountKey.json" if os.path.exists("serviceAccountKey.json") \
                   else "api/serviceAccountKey.json"
        cred = credentials.Certificate(key_path)

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
except Exception as e:
    print(f"LỖI KHỞI TẠO FIREBASE: {e}")

# Khởi tạo thư mục static
os.makedirs("static/avatars", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Cấu hình CORS và Auth
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"],
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/login")

# ══════════════════════════════════════════════════════════════════
#  10. PYDANTIC MODELS
# ══════════════════════════════════════════════════════════════════
class UserRegister(BaseModel):
    username:  str
    email:     str
    password:  str
    full_name: str = ""
    phone:     str = ""

class UserLogin(BaseModel):
    username: str
    password: str

class NewsPost(BaseModel):
    title:       str
    content:     str
    image_url:   str = ""
    category_id: int = 3
class UserUpdate(BaseModel):
    full_name: str
    phone: str
class GoogleFirebaseAuth(BaseModel):
    id_token: str   # Firebase idToken từ Android

# ══════════════════════════════════════════════════════════════════
#  11. AUTH ENDPOINTS
# ══════════════════════════════════════════════════════════════════
@app.get("/", tags=["Health"])
def read_root():
    return {"message":"CryptoPulse API is running!","version":"3.2","docs":"/docs"}


# ── Đăng ký người dùng mới ───────────────────────────────────────
@app.post("/api/v1/auth/register", tags=["Auth"])
async def register_user(user: UserRegister):
    """Đăng ký tài khoản. Mật khẩu được hash bằng bcrypt."""
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Kiểm tra trùng username hoặc email
        cursor.execute(
            "SELECT id FROM users WHERE username=%s OR email=%s",
            (user.username, user.email))
        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Tên đăng nhập hoặc Email đã tồn tại!")

        hashed = hash_password(user.password)
        cursor.execute(
            """INSERT INTO users
               (username, email, password_hash, full_name, phone, role, status)
               VALUES (%s,%s,%s,%s,%s,'user','active')""",
            (user.username, user.email, hashed, user.full_name, user.phone))
        conn.commit()
        user_id = cursor.lastrowid

        token = create_token({
            "sub":   user.username,
            "email": user.email,
            "role":  "user",
        })
        return {
            "status":       "success",
            "message":      "Đăng ký thành công!",
            "access_token": token,
            "token_type":   "bearer",
            "user": {
                "id":        user_id,
                "username":  user.username,
                "email":     user.email,
                "full_name": user.full_name,
                "role":      "user",
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Đăng nhập người dùng (username + password) ───────────────────
@app.post("/api/v1/auth/login", tags=["Auth"])
async def login_user(user: UserLogin):
    """Đăng nhập bằng username + password. Trả về JWT token."""
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM users WHERE username=%s", (user.username,))
        db_user = cursor.fetchone()

        if not db_user or not verify_password(user.password, db_user["password_hash"]):
            raise HTTPException(
                status_code=401,
                detail="Tên đăng nhập hoặc mật khẩu không đúng!")

        if db_user.get("status") == "banned":
            raise HTTPException(status_code=403, detail="Tài khoản đã bị khóa!")

        # Cập nhật last_login
        cursor.execute(
            "UPDATE users SET last_login=NOW() WHERE id=%s", (db_user["id"],))
        conn.commit()

        token = create_token({
            "sub":   db_user["username"],
            "email": db_user.get("email",""),
            "role":  db_user["role"],
        })
        return {
            "status":       "success",
            "access_token": token,
            "token_type":   "bearer",
            "user": {
                "id":         db_user["id"],
                "username":   db_user["username"],
                "email":      db_user.get("email",""),
                "full_name":  db_user.get("full_name",""),
                "phone":      db_user.get("phone",""),
                "role":       db_user["role"],
                "avatar_url": db_user.get("avatar_url",""),
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
GOOGLE_WEB_CLIENT_ID = os.getenv("GOOGLE_WEB_CLIENT_ID", "")
@app.post("/api/v1/auth/google", tags=["Auth"])
async def login_with_google(data: GoogleFirebaseAuth):
    conn = cursor = None
    try:
        try:
            client_id = os.getenv("GOOGLE_WEB_CLIENT_ID") 
            idinfo = id_token.verify_oauth2_token(
                data.id_token, 
                requests.Request(), 
                client_id,
                clock_skew_in_seconds=60 
            )
            email = idinfo['email']
            name  = idinfo.get('name', 'Người dùng CryptoPulse')
            avatar = idinfo.get('picture', '')
            
        except Exception as auth_error:
            print(f"--- [Google Auth Error] ---")
            print(f"Chi tiết: {str(auth_error)}")
            raise HTTPException(status_code=401, detail=str(auth_error))

        # 2. Kết nối Database
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # 3. Kiểm tra hoặc tạo User
        cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
        user = cursor.fetchone()

        if not user:
            # Tạo tài khoản mới nếu chưa có
            username = email.split("@")[0]
            # Đảm bảo username không trùng
            cursor.execute("SELECT id FROM users WHERE username=%s", (username,))
            if cursor.fetchone():
                username = f"{username}_{str(int(time.time()))[-4:]}"

            # Mật khẩu ngẫu nhiên cho tài khoản Google
            rand_pass = secrets.token_hex(16)
            hashed_pass = hash_password(rand_pass)

            cursor.execute(
                """INSERT INTO users 
                   (username, email, password_hash, full_name, role, status, avatar_url) 
                   VALUES (%s, %s, %s, %s, 'user', 'active', %s)""",
                (username, email, hashed_pass, name, avatar)
            )
            conn.commit()
            cursor.execute("SELECT * FROM users WHERE id=%s", (cursor.lastrowid,))
            user = cursor.fetchone()

        # 4. Cập nhật last_login
        cursor.execute("UPDATE users SET last_login=NOW() WHERE id=%s", (user["id"],))
        conn.commit()

        # 5. Tạo JWT nội bộ của CryptoPulse
        access_token = create_token({
            "sub": user["username"],
            "email": user["email"],
            "role": user["role"],
        })
        
        return {
            "status": "success",
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id":         user["id"],
                "username":   user["username"],
                "email":      user["email"],
                "full_name":  user.get("full_name", ""),
                "role":       user["role"],
                "avatar_url": user.get("avatar_url", ""),
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        if conn: conn.rollback()
        print(f"[System Error] {str(e)}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống máy chủ nội bộ")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        
# ── Đăng nhập Admin (OAuth2 form — dùng cho /docs) ───────────────
@app.post("/api/v1/login", tags=["Auth"])
async def login_admin(form_data: OAuth2PasswordRequestForm = Depends()):
    """Đăng nhập admin qua OAuth2 form (Swagger UI)."""
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM users WHERE username=%s AND role='admin'",
            (form_data.username,))
        user = cursor.fetchone()
        if not user or not verify_password(form_data.password, user["password_hash"]):
            raise HTTPException(status_code=400, detail="Sai tài khoản hoặc mật khẩu!")

        token = create_token({"sub": user["username"], "role": "admin"}, days=1)
        return {"access_token": token, "token_type": "bearer"}
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Lấy thông tin user hiện tại ──────────────────────────────────
@app.get("/api/v1/auth/me", tags=["Auth"])
async def get_me(token: str = Depends(oauth2_scheme)):
    """Lấy thông tin tài khoản đang đăng nhập."""
    try:
        payload  = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Token không hợp lệ.")
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT id,username,email,full_name,phone,role,avatar_url,created_at"
            " FROM users WHERE username=%s", (username,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Không tìm thấy user.")
        return {"status":"success","user":user}
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Dependency: lấy current user từ token ────────────────────────
def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {"username": payload.get("sub"), "role": payload.get("role")}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token hết hạn.")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Token không hợp lệ.")

# ── Cập nhật thông tin user (Tên, SĐT) ───────────────────────────
@app.put("/api/v1/auth/me", tags=["Auth"])
async def update_profile(data: UserUpdate, current_user: dict = Depends(get_current_user)):
    """Cập nhật Tên hiển thị và Số điện thoại"""
    conn = cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET full_name=%s, phone=%s WHERE username=%s",
            (data.full_name, data.phone, current_user["username"])
        )
        conn.commit()
        return {"status": "success", "message": "Cập nhật hồ sơ thành công!"}
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()

# ── Upload ảnh đại diện (Avatar) ─────────────────────────────────
class AvatarUpdate(BaseModel):
    avatar_url: str

@app.post("/api/v1/auth/avatar", tags=["Auth"])
async def upload_avatar(
    data: AvatarUpdate,
    current_user: dict = Depends(get_current_user)
):
    """Nhận Cloudinary URL từ Android và lưu vào DB"""
    conn = cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET avatar_url=%s WHERE username=%s",
            (data.avatar_url, current_user["username"])
        )
        conn.commit()
        return {
            "status": "success",
            "message": "Cập nhật ảnh đại diện thành công!",
            "avatar_url": data.avatar_url
        }
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        
# ── Dependency: chỉ admin ─────────────────────────────────────────
def get_admin_user(current: dict = Depends(get_current_user)) -> dict:
    if current.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Không có quyền admin!")
    return current
# ── Xóa ảnh đại diện (Quay về mặc định) ──────────────────────────
@app.delete("/api/v1/auth/avatar", tags=["Auth"])
async def delete_avatar(current_user: dict = Depends(get_current_user)):
    conn = cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Xóa đường dẫn ảnh trong DB (set thành rỗng)
        cursor.execute(
            "UPDATE users SET avatar_url='' WHERE username=%s",
            (current_user["username"],)
        )
        conn.commit()
        return {"status": "success", "message": "Đã xóa ảnh đại diện!"}
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
class GoogleAuth(BaseModel):
    email: str
    name: str
    avatar: str = ""

# ══════════════════════════════════════════════════════════════════
#  12. ADMIN — QUẢN LÝ NGƯỜI DÙNG (CLEAN VERSION)
# ══════════════════════════════════════════════════════════════════

# ── Danh sách tất cả thành viên (Đã xóa cột status) ──────────────
@app.get("/api/v1/admin/users", tags=["Admin"])
async def get_all_users(
    page:    int = Query(1,  ge=1),
    limit:   int = Query(20, le=100),
    search:  str = Query(""),
    admin:   dict = Depends(get_admin_user),
):
    """Lấy danh sách thành viên. Không còn hiển thị trạng thái banned."""
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        offset = (page - 1) * limit

        # Chỉ SELECT những cột cần thiết, bỏ status
        base_query = (
            "SELECT id, username, email, full_name, phone, role, avatar_url, "
            "created_at, last_login FROM users"
        )

        if search:
            like = f"%{search}%"
            cursor.execute(
                f"{base_query} WHERE username LIKE %s OR email LIKE %s OR full_name LIKE %s "
                "ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (like, like, like, limit, offset))
        else:
            cursor.execute(
                f"{base_query} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (limit, offset))

        users = cursor.fetchall()

        cursor.execute("SELECT COUNT(*) AS total FROM users")
        total = cursor.fetchone()["total"]

        return {
            "status": "success",
            "total":  total,
            "page":   page,
            "limit":  limit,
            "users":  users,  
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Chi tiết 1 user (Đã xóa status) ──────────────────────────────
@app.get("/api/v1/admin/users/{user_id}", tags=["Admin"])
async def get_user_detail(
    user_id: int,
    admin:   dict = Depends(get_admin_user),
):
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT id, username, email, full_name, phone, role, avatar_url, "
            "created_at, last_login FROM users WHERE id=%s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Không tìm thấy user.")
        return {"status":"success","user":user}
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()

# ── Đổi role user (Giữ lại vì Admin vẫn cần phân quyền) ──────────
@app.put("/api/v1/admin/users/{user_id}/role", tags=["Admin"])
async def update_user_role(
    user_id: int,
    role:    str = Query(..., description="user | admin"),
    admin:   dict = Depends(get_admin_user),
):
    if role not in ("user","admin"):
        raise HTTPException(status_code=400, detail="Role phải là 'user' hoặc 'admin'")
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET role=%s WHERE id=%s", (role, user_id))
        conn.commit()
        return {"status":"success","message":f"Đã đổi role thành '{role}'!"}
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Xóa user (Admin vẫn có quyền xóa vĩnh viễn) ──────────────────
@app.delete("/api/v1/admin/users/{user_id}", tags=["Admin"])
async def delete_user(
    user_id: int,
    admin:   dict = Depends(get_admin_user),
):
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM users WHERE id=%s AND role!='admin'", (user_id,))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=400, detail="Không thể xóa tài khoản admin!")
        conn.commit()
        return {"status":"success","message":"Đã xóa tài khoản!"}
    except HTTPException:
        raise
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()

@app.get("/api/v1/admin/stats", tags=["Admin"])
async def get_admin_stats(admin: dict = Depends(get_admin_user)):
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT COUNT(*) AS total FROM users")
        total_users = cursor.fetchone()["total"]
        
        cursor.execute("SELECT COUNT(*) AS total FROM news")
        total_news = cursor.fetchone()["total"]
        
        cursor.execute(
            "SELECT COUNT(*) AS total FROM users"
            " WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)")
        new_users_week = cursor.fetchone()["total"]
        
        return {
            "status": "success",
            "stats": {
                "total_users":     total_users,
                "new_users_week":  new_users_week,
                "total_news":      total_news,
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()
# ══════════════════════════════════════════════════════════════════
#  13. MARKET DATA
# ══════════════════════════════════════════════════════════════════
@app.get("/api/v1/market/top-coins", tags=["Market"])
async def get_top_coins():
    data = await async_redis_client.get("top_20_coins_stats")
    if data: return {"status":"success","data":json.loads(data)}
    return {"status":"pending","message":"Đang tải..."}

@app.get("/api/v1/market/fear-greed", tags=["Market"])
async def get_fear_greed():
    data = await async_redis_client.get("market_fear_greed")
    if data: return {"status":"success","data":json.loads(data)}
    return {"status":"pending","message":"Đang tải..."}

# ══════════════════════════════════════════════════════════════════
#  14. KLINE HISTORY
# ══════════════════════════════════════════════════════════════════
VALID_TF = {"1m","5m","15m","1h","4h","1d","1w"}

@app.get("/api/v1/history/{symbol}", tags=["Klines"])
async def get_kline_history(
    symbol:    str = Path(...),
    timeframe: str = Query("1m"),
    end_time:  int = Query(None),
    limit:     int = Query(500, le=5000),
):
    if timeframe not in VALID_TF:
        raise HTTPException(status_code=400, detail=f"Dùng: {VALID_TF}")
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        if end_time:
            cursor.execute(
                f"SELECT * FROM kline_{timeframe} WHERE symbol=%s AND open_time<%s"
                f" ORDER BY open_time DESC LIMIT %s",
                (symbol.upper(), end_time, limit))
        else:
            cursor.execute(
                f"SELECT * FROM kline_{timeframe} WHERE symbol=%s"
                f" ORDER BY open_time DESC LIMIT %s",
                (symbol.upper(), limit))
        data = cursor.fetchall()
        cursor.close(); conn.close()
        data.reverse()
        return {"status":"success","symbol":symbol.upper(),
                "timeframe":timeframe,"count":len(data),"data":data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════
#  15. AI TREND ANALYSIS
# ══════════════════════════════════════════════════════════════════
TF_MAP = {"h1":"1h","1d":"1d","1w":"1w"}

@app.get("/api/v1/analysis/trend/{symbol}", tags=["Analysis"])
async def get_market_trend(symbol: str = Path(...), tf: str = Query("1d")):
    if tf not in TF_MAP:
        raise HTTPException(status_code=400, detail=f"Dùng: {list(TF_MAP)}")
    db_tf = TF_MAP[tf]
    try:
        symbol = symbol.upper()
        if not symbol.endswith("USDT"): symbol += "USDT"

        fng_raw = await async_redis_client.get("market_fear_greed")
        fng_value, fng_label = 50, "Neutral"
        if fng_raw:
            fng_obj = json.loads(fng_raw)
            fng_value = fng_obj.get("value", 50)
            fng_label = fng_obj.get("classification", "Neutral")

        from sqlalchemy import text as _text
        try:
            with _analysis_engine.connect() as _conn:
                df = pd.read_sql(
                    _text(
                        f"SELECT open_price AS open, high_price AS high,"
                        f" low_price AS low, close_price AS close, volume"
                        f" FROM kline_{db_tf} WHERE symbol=:sym"
                        f" ORDER BY open_time DESC LIMIT 100"
                    ),
                    _conn,
                    params={"sym": symbol}
                )
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

        if len(df) < 20:
            raise HTTPException(status_code=404, detail="Không đủ dữ liệu.")
        df = df.iloc[::-1].reset_index(drop=True)

        rsi     = round(float(ta.rsi(df["close"], length=14).iloc[-1]), 2)
        macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
        m_col   = [c for c in macd_df.columns if c.startswith("MACD_")][0]
        ms_col  = [c for c in macd_df.columns if c.startswith("MACDs_")][0]
        mh_col  = [c for c in macd_df.columns if c.startswith("MACDh_")][0]
        macd_val    = round(float(macd_df[m_col].iloc[-1]),  4)
        macd_signal = round(float(macd_df[ms_col].iloc[-1]), 4)
        macd_hist   = round(float(macd_df[mh_col].iloc[-1]), 4)

        bb_df    = ta.bbands(df["close"], length=20)
        bb_upper = round(float(bb_df[[c for c in bb_df.columns if c.startswith("BBU")][0]].iloc[-1]), 2)
        bb_mid   = round(float(bb_df[[c for c in bb_df.columns if c.startswith("BBM")][0]].iloc[-1]), 2)
        bb_lower = round(float(bb_df[[c for c in bb_df.columns if c.startswith("BBL")][0]].iloc[-1]), 2)

        df["ema20"] = ta.ema(df["close"], length=20)
        df["ema50"] = ta.ema(df["close"], length=50) if len(df) >= 50 else df["ema20"]
        ema20 = round(float(df["ema20"].iloc[-1]), 2)
        ema50 = round(float(df["ema50"].iloc[-1]), 2)

        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        atr = round(float(df["atr"].iloc[-1]), 4)

        avg_vol      = float(df["volume"].tail(20).mean())
        last_vol     = float(df["volume"].iloc[-1])
        volume_ratio = round(last_vol / avg_vol, 2) if avg_vol > 0 else 1.0

        cur_p  = float(df["close"].iloc[-1])
        old_p  = float(df["close"].iloc[0])
        pct    = round((cur_p - old_p) / old_p * 100, 2)
        high20 = round(float(df["high"].tail(20).max()), 2)
        low20  = round(float(df["low"].tail(20).min()),  2)

        signals, scores = [], []
        if rsi >= 70:
            signals.append({"indicator":"RSI","value":rsi,"signal":"SELL","note":"Vùng quá mua (>=70)"}); scores.append(20)
        elif rsi >= 55:
            signals.append({"indicator":"RSI","value":rsi,"signal":"BUY","note":"RSI tích cực"}); scores.append(70)
        elif rsi <= 30:
            signals.append({"indicator":"RSI","value":rsi,"signal":"BUY","note":"Vùng quá bán (<=30)"}); scores.append(75)
        elif rsi <= 45:
            signals.append({"indicator":"RSI","value":rsi,"signal":"SELL","note":"RSI yếu"}); scores.append(30)
        else:
            signals.append({"indicator":"RSI","value":rsi,"signal":"NEUTRAL","note":"RSI trung tính (45-55)"}); scores.append(50)

        if macd_val > macd_signal and macd_hist > 0:
            signals.append({"indicator":"MACD","value":macd_val,"signal":"BUY","note":"MACD cắt lên – Golden Cross"}); scores.append(75)
        elif macd_val < macd_signal and macd_hist < 0:
            signals.append({"indicator":"MACD","value":macd_val,"signal":"SELL","note":"MACD cắt xuống – Death Cross"}); scores.append(25)
        else:
            signals.append({"indicator":"MACD","value":macd_val,"signal":"NEUTRAL","note":"MACD hội tụ"}); scores.append(50)

        if ema20 > ema50:
            signals.append({"indicator":"EMA Cross","value":f"{ema20}>{ema50}","signal":"BUY","note":"EMA20 trên EMA50"}); scores.append(70)
        else:
            signals.append({"indicator":"EMA Cross","value":f"{ema20}<{ema50}","signal":"SELL","note":"EMA20 dưới EMA50"}); scores.append(30)

        if cur_p > bb_upper:
            signals.append({"indicator":"Bollinger Bands","value":cur_p,"signal":"SELL","note":f"Giá vượt BB Upper ({bb_upper})"}); scores.append(25)
        elif cur_p < bb_lower:
            signals.append({"indicator":"Bollinger Bands","value":cur_p,"signal":"BUY","note":f"Giá dưới BB Lower ({bb_lower})"}); scores.append(75)
        else:
            pos = round((cur_p-bb_lower)/(bb_upper-bb_lower)*100,1) if bb_upper!=bb_lower else 50
            signals.append({"indicator":"Bollinger Bands","value":cur_p,"signal":"NEUTRAL","note":f"Trong dải BB ({pos}%)"}); scores.append(50)

        if volume_ratio >= 1.5:
            signals.append({"indicator":"Volume","value":round(last_vol,2),"signal":"STRONG","note":f"Volume cao ({volume_ratio}x TB)"})
        elif volume_ratio <= 0.5:
            signals.append({"indicator":"Volume","value":round(last_vol,2),"signal":"WEAK","note":f"Volume thấp ({volume_ratio}x TB)"})
        else:
            signals.append({"indicator":"Volume","value":round(last_vol,2),"signal":"NORMAL","note":f"Volume bình thường ({volume_ratio}x TB)"})

        if fng_value >= 75:
            signals.append({"indicator":"Fear & Greed","value":fng_value,"signal":"SELL","note":f"Tham lam cực độ ({fng_label})"}); scores.append(25)
        elif fng_value >= 55:
            signals.append({"indicator":"Fear & Greed","value":fng_value,"signal":"BUY","note":f"Tham lam ({fng_label})"}); scores.append(65)
        elif fng_value <= 25:
            signals.append({"indicator":"Fear & Greed","value":fng_value,"signal":"BUY","note":f"Sợ hãi cực độ ({fng_label})"}); scores.append(70)
        else:
            signals.append({"indicator":"Fear & Greed","value":fng_value,"signal":"NEUTRAL","note":f"Trung tính ({fng_label})"}); scores.append(50)

        final = round(sum(scores)/len(scores), 2)
        if final >= 72:   trend,action,risk = "Tăng mạnh (Strong Bullish)","NÊN MUA – Xu hướng tăng rõ ràng","Thấp"
        elif final >= 58: trend,action,risk = "Tăng nhẹ (Bullish)","CÓ THỂ MUA – Cần theo dõi thêm","Trung bình"
        elif final >= 45: trend,action,risk = "Đi ngang (Neutral)","QUAN SÁT – Chờ tín hiệu rõ hơn","Trung bình"
        elif final >= 30: trend,action,risk = "Giảm nhẹ (Bearish)","THẬN TRỌNG – Hạn chế mua mới","Cao"
        else:             trend,action,risk = "Giảm mạnh (Strong Bearish)","NÊN BÁN / TRÁNH MUA","Rất cao"

        atr_pct    = round(atr/cur_p*100, 2)
        volatility = ("Rất cao" if atr_pct>5 else "Cao" if atr_pct>3
                      else "Trung bình" if atr_pct>1.5 else "Thấp")

        return {
            "symbol":symbol,"timeframe":tf,
            "price":{"current":round(cur_p,4),"change":round(cur_p-old_p,4),
                     "change_pct":pct,"high_20":high20,"low_20":low20,
                     "support":low20,"resistance":high20},
            "indicators":{"rsi":rsi,"macd":macd_val,"macd_signal":macd_signal,
                          "macd_hist":macd_hist,"ema20":ema20,"ema50":ema50,
                          "bb_upper":bb_upper,"bb_mid":bb_mid,"bb_lower":bb_lower,
                          "atr":atr,"atr_pct":atr_pct,"volume_ratio":volume_ratio},
            "market_sentiment":{"fear_greed_value":fng_value,"fear_greed_label":fng_label},
            "analysis":{"score":final,"trend":trend,"action":action,"risk_level":risk,
                        "volatility":volatility,
                        "buy_signals":    sum(1 for s in signals if s["signal"]=="BUY"),
                        "sell_signals":   sum(1 for s in signals if s["signal"]=="SELL"),
                        "neutral_signals":sum(1 for s in signals if s["signal"]=="NEUTRAL")},
            "signals":signals,
            "timestamp":datetime.now(timezone.utc).isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════
#  16. ON-CHAIN HELPERS
# ══════════════════════════════════════════════════════════════════
COINGECKO_IDS = {
    "USDT":"tether","USDC":"usd-coin","BUSD":"binance-usd","DAI":"dai",
    "WETH":"weth","ETH":"ethereum","BTC":"bitcoin","WBTC":"wrapped-bitcoin",
    "BNB":"binancecoin","WBNB":"wbnb","SOL":"solana","MATIC":"matic-network",
    "POL":"matic-network","AVAX":"avalanche-2","WAVAX":"avalanche-2",
    "LINK":"chainlink","UNI":"uniswap","AAVE":"aave","CRV":"curve-dao-token",
    "COMP":"compound-governance-token","MKR":"maker","SNX":"havven",
    "SUSHI":"sushi","1INCH":"1inch","LDO":"lido-dao","GMX":"gmx",
    "SHIB":"shiba-inu","DOGE":"dogecoin","ARB":"arbitrum","OP":"optimism",
    "INJ":"injective-protocol","SUI":"sui","APT":"aptos","NEAR":"near",
    "ATOM":"cosmos","DOT":"polkadot","LTC":"litecoin","BCH":"bitcoin-cash",
    "TRX":"tron","XRP":"ripple","ADA":"cardano","FTM":"fantom",
    "PEPE":"pepe","FLOKI":"floki","BONK":"bonk",
}

async def get_price_from_binance(symbol: str) -> float:
    if not symbol or symbol in ("null","None",""): return 0.0
    for quote in ("USDT","BUSD"):
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                res = await client.get("https://api.binance.com/api/v3/ticker/price",
                                       params={"symbol":symbol.upper()+quote})
            if res.status_code == 200:
                p = float(res.json().get("price",0))
                if p > 0: return p
        except Exception: pass
    return 0.0

async def _get_token_price_safe(headers,chain,contract,block,symbol,timestamp="") -> float:
    if contract and contract not in ("null","None","",ZERO_ADDRESS):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(f"{MORALIS_BASE}/erc20/{contract}/price",
                                       headers=headers,params={"chain":chain,"to_block":block})
            if res.status_code == 200:
                p = float(res.json().get("usdPrice") or 0)
                if p > 0: return p
        except Exception as e:
            print(f"[PriceT1] {symbol}: {e}")
    cg_id = COINGECKO_IDS.get(symbol.upper(),"")
    if cg_id and timestamp and len(timestamp) >= 10:
        try:
            parts = timestamp[:10].split("-")
            cg_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
            async with httpx.AsyncClient(timeout=10) as client:
                res = await client.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}/history",
                    params={"date":cg_date,"localization":"false"})
            if res.status_code == 200:
                p = float((res.json().get("market_data",{}).get("current_price") or {}).get("usd") or 0)
                if p > 0: return p
            if res.status_code == 429:
                await asyncio.sleep(1)
                async with httpx.AsyncClient(timeout=10) as client:
                    res2 = await client.get(
                        f"https://api.coingecko.com/api/v3/coins/{cg_id}/history",
                        params={"date":cg_date,"localization":"false"})
                if res2.status_code == 200:
                    p = float((res2.json().get("market_data",{}).get("current_price") or {}).get("usd") or 0)
                    if p > 0: return p
        except Exception as e:
            print(f"[PriceT2] {symbol}: {e}")
    return await get_price_from_binance(symbol)

def _direction(from_addr,to_addr,wallet):
    w,f,t = wallet.lower(),(from_addr or "").lower(),(to_addr or "").lower()
    if f == ZERO_ADDRESS: return "mint"
    if f == w == t:       return "internal"
    if f == w:            return "sent"
    return "received"

def _parse_native_txs(raw,wallet):
    result = []
    for tx in raw:
        try:    amount = float(tx.get("value","0"))/1e18
        except: amount = 0.0
        if amount == 0: continue
        result.append({
            "type":"native","direction":_direction(tx.get("from_address",""),tx.get("to_address",""),wallet),
            "tx_hash":tx.get("hash"),"block_number":tx.get("block_number"),
            "from":tx.get("from_address",""),"to":tx.get("to_address",""),
            "token_symbol":None,"token_name":None,"token_contract":None,
            "amount":round(amount,8),"price_at_tx":None,"usd_value":None,
            "block_timestamp":tx.get("block_timestamp"),"receipt_status":tx.get("receipt_status"),
        })
    return result

def _parse_token_transfers(raw,wallet):
    result = []
    for tx in raw:
        try:
            dec = int(tx.get("token_decimals") or tx.get("decimals") or 18)
            amount = int(tx.get("value",0))/(10**dec)
        except: amount = 0.0
        result.append({
            "type":"token_transfer","direction":_direction(tx.get("from_address",""),tx.get("to_address",""),wallet),
            "tx_hash":tx.get("transaction_hash"),"block_number":tx.get("block_number"),
            "from":tx.get("from_address",""),"to":tx.get("to_address",""),
            "token_symbol":tx.get("token_symbol"),"token_name":tx.get("token_name"),
            "token_contract":tx.get("address"),"amount":round(amount,6),
            "price_at_tx":None,"usd_value":None,
            "block_timestamp":tx.get("block_timestamp"),"receipt_status":"1",
        })
    return result

# ══════════════════════════════════════════════════════════════════
#  17. ON-CHAIN WALLET ENDPOINT
# ══════════════════════════════════════════════════════════════════
@app.get("/api/v1/onchain/wallet/{chain}/{address}", tags=["On-chain"])
async def get_wallet_txs(
    address:       str  = Path(...),
    chain:         str  = Path(...),
    force_refresh: bool = Query(False),
):
    cache_key = f"cache:onchain:wallet:{chain.lower()}:{address.lower()}"
    try:
        if not force_refresh:
            cached = await async_redis_client.get(cache_key)
            if cached:
                d = json.loads(cached); d["is_cached"] = True; return d

        headers  = {"accept":"application/json","X-API-Key":MORALIS_API_KEY}
        chain_id = chain.lower(); wallet = address.lower()

        async with httpx.AsyncClient(timeout=30) as client:
            n_res,t_res,b_res,nb_res = await asyncio.gather(
                client.get(f"{MORALIS_BASE}/{wallet}",               headers=headers,params={"chain":chain_id,"limit":15}),
                client.get(f"{MORALIS_BASE}/{wallet}/erc20/transfers",headers=headers,params={"chain":chain_id,"limit":15}),
                client.get(f"{MORALIS_BASE}/{wallet}/erc20",         headers=headers,params={"chain":chain_id}),
                client.get(f"{MORALIS_BASE}/{wallet}/balance",       headers=headers,params={"chain":chain_id}),
            )

        native_txs = _parse_native_txs(n_res.json().get("result",[]), wallet)
        token_txs  = _parse_token_transfers(t_res.json().get("result",[]), wallet)

        prices = await asyncio.gather(*[
            _get_token_price_safe(headers,chain_id,
                tx.get("token_contract",""),tx.get("block_number",""),
                tx.get("token_symbol",""),tx.get("block_timestamp",""))
            for tx in token_txs
        ])
        for i,p in enumerate(prices):
            try:    a = float(token_txs[i].get("amount") or 0)
            except: a = 0.0
            token_txs[i]["price_at_tx"] = round(p,6)
            token_txs[i]["usd_value"]   = round(a*p,2)

        total_usd,assets = 0.0,[]
        native_sym_map = {"eth":"ETH","bsc":"BNB","polygon":"MATIC","avalanche":"AVAX",
                          "arbitrum":"ETH","optimism":"ETH","base":"ETH","fantom":"FTM","cronos":"CRO"}
        if nb_res.status_code == 200:
            raw_bal = nb_res.json().get("balance","0")
            if raw_bal and raw_bal != "0":
                sym = native_sym_map.get(chain_id,"ETH")
                bal = int(raw_bal)/1e18
                if bal > 0:
                    price = await get_price_from_binance(sym)
                    usd_val = bal*price; total_usd += usd_val
                    assets.append({"symbol":sym,"name":sym,"balance":round(bal,6),"usd_value":round(usd_val,2)})

        if b_res.status_code == 200:
            for b in b_res.json():
                try:
                    dec = int(b.get("decimals") or 18)
                    bal = int(b.get("balance") or 0)/(10**dec)
                    usd = float(b.get("usd_value") or 0)
                    if bal > 0:
                        total_usd += usd
                        assets.append({"symbol":b.get("symbol",""),"name":b.get("name",""),
                                       "balance":round(bal,6),"usd_value":round(usd,2)})
                except Exception: pass

        merged = {tx["tx_hash"]:tx for tx in native_txs if tx.get("tx_hash")}
        for tx in token_txs:
            if tx.get("tx_hash"): merged[tx["tx_hash"]] = tx
        all_tx = sorted(merged.values(),key=lambda x:x.get("block_timestamp",""),reverse=True)

        seen,token_summary = set(),[]
        for tx in token_txs:
            c = tx.get("token_contract")
            if c and c not in seen:
                seen.add(c)
                token_summary.append({"token_symbol":tx["token_symbol"],
                                      "token_name":tx["token_name"],"token_contract":c})

        result = {
            "status":"success","network":chain.upper(),"address":address,"is_cached":False,
            "portfolio":{"total_usd":round(total_usd,2),"tokens":assets},
            "stats":{"total":len(all_tx),
                     "sent":    sum(1 for t in all_tx if t.get("direction")=="sent"),
                     "received":sum(1 for t in all_tx if t.get("direction") in ("received","mint"))},
            "transactions":all_tx,"tokens_involved":token_summary,"total":len(all_tx),
        }
        await async_redis_client.setex(cache_key,3600,json.dumps(result))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ══════════════════════════════════════════════════════════════════
#  18. NEWS ENDPOINTS
# ══════════════════════════════════════════════════════════════════

# ── Models ───────────────────────────────────────────────────────
class HideNewsRequest(BaseModel):
    url: str

# ── Tin nội bộ đã duyệt ──────────────────────────────────────────
@app.get("/api/v1/news", tags=["News"])
async def get_all_news():
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM news WHERE status='published'"
            " ORDER BY created_at DESC LIMIT 20")
        return {"status": "success", "data": cursor.fetchall()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Admin: Đăng tin nội bộ ───────────────────────────────────────
@app.post("/api/v1/news/admin/post", tags=["News"])
async def admin_post_news(
    news:  NewsPost,
    admin: dict = Depends(get_admin_user),
):
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO news(title,content,image_url,author,category_id,status)"
            " VALUES(%s,%s,%s,%s,%s,'published')",
            (news.title, news.content, news.image_url,
             admin["username"], news.category_id))
        conn.commit()
        return {
            "status":  "success",
            "news_id": cursor.lastrowid,
            "message": "Đã đăng tin thành công!",
        }
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Admin: Sửa tin nội bộ ────────────────────────────────────────
@app.put("/api/v1/news/admin/{news_id}", tags=["News"])
async def admin_update_news(
    news_id: int,
    news:    NewsPost,
    admin:   dict = Depends(get_admin_user),
):
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM news WHERE id=%s", (news_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Không tìm thấy bài viết!")
        cursor.execute(
            "UPDATE news SET title=%s,content=%s,image_url=%s,category_id=%s"
            " WHERE id=%s",
            (news.title, news.content, news.image_url,
             news.category_id, news_id))
        conn.commit()
        return {"status": "success", "message": f"Đã cập nhật bài viết #{news_id}"}
    except HTTPException:
        raise
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Admin: Xóa tin nội bộ (DB) ───────────────────────────────────
@app.delete("/api/v1/news/admin/{news_id}", tags=["News"])
async def admin_delete_news(
    news_id: int,
    admin:   dict = Depends(get_admin_user),
):
    conn = cursor = None
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM news WHERE id=%s", (news_id,))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Không tìm thấy bài viết!")
        conn.commit()
        return {"status": "success", "message": "Đã xóa bài viết!"}
    except HTTPException:
        raise
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


# ── Admin: Chặn vĩnh viễn tin Whale theo URL (Blacklist) ─────────
@app.post("/api/v1/admin/news/hide", tags=["News"])
async def hide_whale_news(
    req:   HideNewsRequest,
    admin: dict = Depends(get_admin_user),
):
    """
    Thêm URL vào Danh sách đen Redis.
    Bot có kéo lại bài này thì cũng không hiện lên app.
    """
    if not req.url or not req.url.startswith("http"):
        raise HTTPException(status_code=400, detail="URL không hợp lệ!")
    try:
        await async_redis_client.sadd("hidden_whale_news", req.url)
        return {
            "status":  "success",
            "message": "Đã chặn bài báo này vĩnh viễn!",
            "url":     req.url,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Admin: Xem danh sách đen ─────────────────────────────────────
@app.get("/api/v1/admin/news/hidden", tags=["News"])
async def get_hidden_news(admin: dict = Depends(get_admin_user)):
    """Xem tất cả URL đang bị chặn."""
    try:
        raw = await async_redis_client.smembers("hidden_whale_news")
        urls = [u.decode("utf-8") if isinstance(u, bytes) else u
                for u in (raw or [])]
        return {"status": "success", "count": len(urls), "hidden_urls": urls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Admin: Bỏ chặn một bài (xóa khỏi blacklist) ──────────────────
@app.delete("/api/v1/admin/news/hidden", tags=["News"])
async def unhide_whale_news(
    req:   HideNewsRequest,
    admin: dict = Depends(get_admin_user),
):
    """Khôi phục bài bị ẩn — xóa URL khỏi Danh sách đen."""
    try:
        removed = await async_redis_client.srem("hidden_whale_news", req.url)
        if removed == 0:
            raise HTTPException(
                status_code=404, detail="URL này không có trong danh sách đen!")
        return {"status": "success", "message": "Đã bỏ chặn bài báo này!"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Whale news (tích hợp bộ lọc Blacklist) ───────────────────────
@app.get("/api/v1/news/whale", tags=["News"])
async def get_whale_news(
    force_refresh: bool = Query(False),
    coin:          str  = Query(None),
    limit:         int  = Query(20, le=40),
):
    """Tin Whale đã dịch tiếng Việt, tự động lọc bỏ URL trong Blacklist."""
    try:
        if force_refresh:
            payload = await asyncio.wait_for(
                _build_whale_news_payload(), timeout=300)
            await async_redis_client.setex(
                "whale_institutional_news", 900,
                json.dumps(payload, ensure_ascii=False))
        else:
            cached  = await async_redis_client.get("whale_institutional_news")
            payload = json.loads(cached) if cached \
                      else await _build_whale_news_payload()
            if not cached:
                await async_redis_client.setex(
                    "whale_institutional_news", 900,
                    json.dumps(payload, ensure_ascii=False))

        data = payload.get("data", [])

        # ── Lọc Blacklist ─────────────────────────────────────
        raw_hidden = await async_redis_client.smembers("hidden_whale_news")
        if raw_hidden:
            blacklist = {
                u.decode("utf-8") if isinstance(u, bytes) else u
                for u in raw_hidden
            }
            data = [n for n in data if n.get("url") not in blacklist]

        # ── Lọc theo coin ─────────────────────────────────────
        if coin:
            data = [n for n in data
                    if coin.upper() in (n.get("coins") or [])]

        return {
            "status":      "success",
            "count":       len(data[:limit]),
            "updated_at":  payload.get("updated_at"),
            "filter_coin": coin,
            "data":        data[:limit],
        }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=503, detail="Đang xử lý, thử lại sau.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Thống kê coin trong Whale news ───────────────────────────────
@app.get("/api/v1/news/whale/coins", tags=["News"])
async def get_whale_news_coins():
    cached = await async_redis_client.get("whale_institutional_news")
    if not cached:
        return {"status": "pending", "message": "Chưa có dữ liệu..."}
    payload    = json.loads(cached)
    coin_count = {}
    for item in payload.get("data", []):
        for c in (item.get("coins") or []):
            coin_count[c] = coin_count.get(c, 0) + 1
    return {
        "status":     "success",
        "updated_at": payload.get("updated_at"),
        "coins": [{"coin": k, "count": v}
                  for k, v in sorted(
                      coin_count.items(),
                      key=lambda x: x[1], reverse=True)],
    }


# ── Trigger crawl thủ công ────────────────────────────────────────
@app.post("/api/v1/news/crawl", tags=["News"])
async def trigger_crawl(admin: dict = Depends(get_admin_user)):
    asyncio.create_task(crawl_worker_once())
    return {"status": "success",
            "message": "Đang crawl & dịch, vui lòng chờ ~30 giây."}


async def crawl_worker_once():
    """Crawl CoinDesk, dịch tiếng Việt, bỏ qua URL trong Blacklist."""
    try:
        async with httpx.AsyncClient(
            timeout=15, headers={"User-Agent": "Mozilla/5.0"}
        ) as client:
            res = await client.get(
                "https://www.coindesk.com/arc/outboundfeeds/rss/")
        if res.status_code != 200:
            return

        # Đọc blacklist trước khi insert
        raw_hidden = await async_redis_client.smembers("hidden_whale_news")
        blacklist  = {
            u.decode("utf-8") if isinstance(u, bytes) else u
            for u in (raw_hidden or [])
        }

        feed = feedparser.parse(res.text)
        conn = cursor = None
        try:
            conn   = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            inserted = 0
            for entry in feed.entries[:10]:
                url = entry.get("link", "")
                if not url:
                    continue
                # Bỏ qua URL trong Blacklist
                if url in blacklist:
                    print(f"[Crawl] Bỏ qua URL bị chặn: {url[:60]}")
                    continue
                cursor.execute(
                    "SELECT id FROM news WHERE original_url=%s", (url,))
                if cursor.fetchone():
                    continue

                title_vi   = await asyncio.get_event_loop().run_in_executor(
                    None, lambda t=entry.get("title", ""):
                    _translate_vi(t, 200))
                content_vi = await asyncio.get_event_loop().run_in_executor(
                    None, lambda c=entry.get("summary", ""):
                    _translate_vi(c, 500))

                image_url = ""
                media = entry.get("media_content", [])
                if media:
                    image_url = media[0].get("url", "")
                if not image_url:
                    thumb = entry.get("media_thumbnail", [])
                    if thumb:
                        image_url = thumb[0].get("url", "")

                cursor.execute(
                    "INSERT INTO news"
                    "(title,content,image_url,original_url,author,status,category_id)"
                    " VALUES(%s,%s,%s,%s,%s,'published',3)",
                    (title_vi, content_vi, image_url, url, "Bot (CoinDesk)"))
                inserted += 1
                await asyncio.sleep(0.5)

            conn.commit()
            print(f"[Crawl-Once] ✓ {inserted} bài mới.")
        except Exception as e:
            if conn: conn.rollback()
            print(f"[Crawl-Once] ❌ {e}")
        finally:
            if cursor: cursor.close()
            if conn:   conn.close()
    except Exception as e:
        print(f"[Crawl-Once] ❌ {e}")

# ══════════════════════════════════════════════════════════════════
#  19. WEBSOCKET
# ══════════════════════════════════════════════════════════════════
@app.websocket("/ws/crypto/{symbol}")
async def ws_crypto(websocket: WebSocket, symbol: str):
    await websocket.accept()
    symbol    = symbol.upper()
    redis_key = f"kline:latest:{symbol}"
    try:
        last_price = None
        while True:
            data = await async_redis_client.hgetall(redis_key)
            if data and data.get("price") != last_price:
                await websocket.send_json({
                    "event":"kline_update","symbol":symbol,
                    "price":float(data["price"]),"time":int(data.get("time",0)),
                })
                last_price = data["price"]
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        print(f"[-] WS ngắt: {symbol}")
    except Exception as e:
        print(f"[!] WS lỗi {symbol}: {e}")
@app.get("/health")
def health():
    return {"status": "ok"}