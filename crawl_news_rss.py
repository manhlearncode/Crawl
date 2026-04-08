from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import feedparser
import pandas as pd
import requests
import trafilatura
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# RSS chính thức: giữ nguyên VnExpress, Thanh Niên; bổ sung Dân trí
RSS_FEEDS: Dict[str, List[str]] = {
    "thoi_su": [
        "https://vnexpress.net/rss/thoi-su.rss",
        "https://thanhnien.vn/rss/thoi-su.rss",
        "https://dantri.com.vn/rss/thoi-su.rss",
    ],
    "kinh_doanh": [
        "https://vnexpress.net/rss/kinh-doanh.rss",
        "https://thanhnien.vn/rss/kinh-te.rss",
        "https://dantri.com.vn/rss/kinh-doanh.rss",
    ],
    "the_gioi": [
        "https://vnexpress.net/rss/the-gioi.rss",
        "https://thanhnien.vn/rss/the-gioi.rss",
        "https://dantri.com.vn/rss/the-gioi.rss",
    ],
    "giao_duc": [
        "https://vnexpress.net/rss/giao-duc.rss",
        "https://thanhnien.vn/rss/giao-duc.rss",
        "https://dantri.com.vn/rss/giao-duc.rss",
    ],
    "phap_luat": [
        "https://vnexpress.net/rss/phap-luat.rss",
        "https://thanhnien.vn/rss/thoi-su/phap-luat.rss",
        "https://dantri.com.vn/rss/phap-luat.rss",
    ],
    "the_thao": [
        "https://vnexpress.net/rss/the-thao.rss",
        "https://thanhnien.vn/rss/the-thao.rss",
        "https://dantri.com.vn/rss/the-thao.rss",
    ],
    "suc_khoe": [
        "https://vnexpress.net/rss/suc-khoe.rss",
        "https://thanhnien.vn/rss/suc-khoe.rss",
        "https://dantri.com.vn/rss/suc-khoe.rss",
    ],
    "cong_nghe": [
        "https://vnexpress.net/rss/khoa-hoc-cong-nghe.rss",
        "https://thanhnien.vn/rss/cong-nghe.rss",
        "https://dantri.com.vn/rss/cong-nghe.rss",
    ],
    "du_lich": [
        "https://vnexpress.net/rss/du-lich.rss",
        "https://thanhnien.vn/rss/du-lich.rss",
        "https://dantri.com.vn/rss/du-lich.rss",
    ],
    "giai_tri": [
        "https://vnexpress.net/rss/giai-tri.rss",
        "https://thanhnien.vn/rss/giai-tri.rss",
        "https://dantri.com.vn/rss/giai-tri.rss",
    ],
}

LABEL_TO_SLUG_DANTRI = {
    "thoi_su": "thoi-su",
    "kinh_doanh": "kinh-doanh",
    "the_gioi": "the-gioi",
    "giao_duc": "giao-duc",
    "phap_luat": "phap-luat",
    "the_thao": "the-thao",
    "suc_khoe": "suc-khoe",
    "cong_nghe": "cong-nghe",
    "du_lich": "du-lich",
    "giai_tri": "giai-tri",
}

LABEL_TO_SLUG_TUOITRE = {
    "thoi_su": "thoi-su",
    "kinh_doanh": "kinh-doanh",
    "the_gioi": "the-gioi",
    "giao_duc": "giao-duc",
    "phap_luat": "phap-luat",
    "the_thao": "the-thao",
    "suc_khoe": "suc-khoe",
    "cong_nghe": "cong-nghe",
    "du_lich": "du-lich",
    "giai_tri": "giai-tri",
}

LABEL_TO_SLUG_VIETNAMNET = {
    "thoi_su": "thoi-su",
    "kinh_doanh": "kinh-doanh",
    "the_gioi": "the-gioi",
    "giao_duc": "giao-duc",
    "phap_luat": "phap-luat",
    "the_thao": "the-thao",
    "suc_khoe": "suc-khoe",
    "cong_nghe": "cong-nghe",
    "du_lich": "du-lich",
    "giai_tri": "van-hoa-giai-tri",
}


def build_listing_sources() -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for label in RSS_FEEDS.keys():
        out[label] = []

        slug_dt = LABEL_TO_SLUG_DANTRI[label]
        out[label].append(
            {
                "name": "dantri",
                "first_page_url": f"https://dantri.com.vn/{slug_dt}.htm",
                "page_url_template": f"https://dantri.com.vn/{slug_dt}/trang-{{page}}.htm",
                "article_regex": r"^https?://dantri\.com\.vn/.+-\d{10,}\.htm$",
            }
        )

        slug_tt = LABEL_TO_SLUG_TUOITRE[label]
        out[label].append(
            {
                "name": "tuoitre",
                "first_page_url": f"https://tuoitre.vn/{slug_tt}.htm",
                "page_url_template": f"https://tuoitre.vn/{slug_tt}/trang-{{page}}.htm",
                "article_regex": r"^https?://tuoitre\.vn/.+-\d{10,}\.htm$",
            }
        )

        slug_vnn = LABEL_TO_SLUG_VIETNAMNET[label]
        out[label].append(
            {
                "name": "vietnamnet",
                "first_page_url": f"https://vietnamnet.vn/{slug_vnn}",
                "page_url_template": f"https://vietnamnet.vn/{slug_vnn}-page{{page0}}",
                "article_regex": r"^https?://vietnamnet\.vn/.+-\d{6,}\.html$",
            }
        )

    return out


LISTING_SOURCES = build_listing_sources()


BAD_URL_PARTS = (
    "/video/",
    "/infographic/",
    "/photo/",
    "/podcast/",
    "/tag/",
    "/tags/",
    "/tim-kiem",
    "/rss",
    "facebook.com",
    "doubleclick.net",
    "googleads.g.doubleclick.net",
    "googlesyndication.com",
    "youtube.com",
    "tiktok.com",
    "zalo.me",
)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def strip_html_tags(text: str) -> str:
    if not text:
        return ""
    # Một số feed đưa plain text, không phải HTML; parse bằng BS4 sẽ tạo cảnh báo không cần thiết.
    if "<" not in text and ">" not in text:
        return normalize_whitespace(text)
    soup = BeautifulSoup(text, "html.parser")
    return normalize_whitespace(soup.get_text(" "))


def normalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return clean.rstrip("/")


def safe_filename_from_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def fix_mojibake(text: str) -> str:
    if not text:
        return ""
    bad_patterns = ("Ã", "Â", "Ä", "á»", "áº", "â€")
    if any(x in text for x in bad_patterns):
        try:
            return text.encode("latin1").decode("utf-8")
        except Exception:
            return text
    return text


def fetch_url_text(url: str, timeout: int = 20) -> Optional[str]:
    try:
        resp = SESSION.get(url, timeout=timeout)
        resp.raise_for_status()
        raw = resp.content

        for enc in ("utf-8", "utf-8-sig", resp.encoding):
            if not enc:
                continue
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                continue

        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def fetch_feed(feed_url: str) -> List[dict]:
    try:
        resp = SESSION.get(feed_url, timeout=20)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        return parsed.entries
    except Exception as e:
        print(f"[WARN] Không đọc được feed: {feed_url} | {e}")
        return []


def extract_with_trafilatura(url: str, html_text: str) -> Tuple[str, str]:
    try:
        extracted = trafilatura.extract(
            html_text,
            url=url,
            favor_precision=True,
            include_comments=False,
            include_tables=False,
            include_images=False,
        )
        title = ""
        metadata = trafilatura.extract_metadata(html_text, default_url=url)
        if metadata and getattr(metadata, "title", None):
            title = normalize_whitespace(metadata.title)
        content = normalize_whitespace(extracted or "")
        return title, content
    except Exception:
        return "", ""


def extract_fallback(url: str, html_text: str) -> Tuple[str, str, str]:
    soup = BeautifulSoup(html_text, "html.parser")

    title = ""
    if soup.title:
        title = normalize_whitespace(soup.title.get_text(" "))

    sapo = ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        sapo = normalize_whitespace(meta_desc["content"])

    paragraphs = []
    selectors = [
        "article p",
        ".article__body p",
        ".fck_detail p",
        ".detail-content p",
        ".content-detail p",
        ".maincontent p",
        ".Normal",
        ".detail__content p",
        ".box-content-detail p",
        ".content p",
        ".article-content p",
    ]
    seen = set()

    for selector in selectors:
        for p in soup.select(selector):
            txt = normalize_whitespace(p.get_text(" "))
            if txt and txt not in seen and len(txt) > 20:
                seen.add(txt)
                paragraphs.append(txt)
        if paragraphs:
            break

    content = "\n".join(paragraphs)
    content = normalize_whitespace(content)
    return title, sapo, content


def extract_article(url: str, feed_title: str = "", feed_summary: str = "") -> Optional[dict]:
    html_text = fetch_url_text(url)
    if not html_text:
        return None

    title_tra, content_tra = extract_with_trafilatura(url, html_text)
    fb_title, fb_sapo, fb_content = extract_fallback(url, html_text)

    title = title_tra or feed_title or fb_title
    sapo = feed_summary or fb_sapo
    content = content_tra or fb_content

    title = normalize_whitespace(html.unescape(fix_mojibake(title)))
    sapo = normalize_whitespace(html.unescape(strip_html_tags(fix_mojibake(sapo))))
    content = normalize_whitespace(html.unescape(fix_mojibake(content)))

    if len(content) < 120:
        return None

    return {
        "title": title,
        "sapo": sapo,
        "content": content,
        "url": normalize_url(url),
    }


def save_article_json(out_dir: Path, label: str, article: dict) -> Path:
    label_dir = out_dir / label
    label_dir.mkdir(parents=True, exist_ok=True)

    filename = safe_filename_from_url(article["url"]) + ".json"
    path = label_dir / filename
    path.write_text(json.dumps(article, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def is_probable_article_url(url: str, article_regex: Optional[str] = None) -> bool:
    if not url:
        return False

    url = normalize_url(url)
    lower = url.lower()

    if any(part in lower for part in BAD_URL_PARTS):
        return False

    if article_regex:
        return re.match(article_regex, url) is not None

    return any(
        [
            lower.endswith(".htm"),
            lower.endswith(".html"),
            lower.endswith(".ldo"),
            lower.endswith(".tpo"),
        ]
    )


def collect_links_from_listing(listing_url: str, article_regex: Optional[str] = None) -> List[str]:
    html_text = fetch_url_text(listing_url)
    if not html_text:
        return []

    lower_html = html_text.lower()
    if "document.cookie=\"d1n=" in lower_html and "window.location.reload" in lower_html:
        print(f"[WARN] Listing bị anti-bot/challenge: {listing_url}")
        return []
    if "doubleclick.net/activityi" in lower_html:
        print(f"[WARN] Listing bị chuyển hướng quảng cáo/tracking: {listing_url}")
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    links: List[str] = []
    seen: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full_url = normalize_url(urljoin(listing_url, href))

        if full_url in seen:
            continue
        if not is_probable_article_url(full_url, article_regex=article_regex):
            continue

        seen.add(full_url)
        links.append(full_url)

    return links


def resolve_listing_page_url(source: dict, page: int) -> str:
    first_page_url = source.get("first_page_url")
    if page == 1 and first_page_url:
        return first_page_url

    page_url_template = source.get("page_url_template")
    if not page_url_template:
        raise ValueError(f"Nguồn listing thiếu page_url_template: {source.get('name', 'unknown')}")

    return page_url_template.format(page=page, page0=page - 1)


def crawl_rss_sources(
    label: str,
    feed_urls: List[str],
    out_dir: Path,
    per_label: int,
    sleep_sec: float,
    seen_urls: Set[str],
) -> List[dict]:
    collected = []

    for feed_url in feed_urls:
        if len(collected) >= per_label:
            break

        print(f"\n[INFO] RSS | nhãn='{label}' | {feed_url}")
        entries = fetch_feed(feed_url)

        for entry in entries:
            if len(collected) >= per_label:
                break

            link = normalize_url(entry.get("link", "").strip())
            if not link or link in seen_urls:
                continue

            feed_title = normalize_whitespace(fix_mojibake(entry.get("title", "")))
            feed_summary = strip_html_tags(fix_mojibake(entry.get("summary", "")))

            article = extract_article(
                url=link,
                feed_title=feed_title,
                feed_summary=feed_summary,
            )
            time.sleep(sleep_sec)

            if article is None:
                continue

            article["label"] = label
            article["source_feed"] = feed_url
            article["source_type"] = "rss"
            article["published"] = entry.get("published", "") or entry.get("updated", "")

            path = save_article_json(out_dir, label, article)
            article["saved_file"] = str(path)

            seen_urls.add(link)
            collected.append(article)
            print(f"[OK][RSS] {label}: {len(collected)}/{per_label} | {article['title'][:90]}")

    return collected


def crawl_listing_sources(
    label: str,
    sources: List[dict],
    out_dir: Path,
    per_label: int,
    sleep_sec: float,
    seen_urls: Set[str],
    already_collected: int,
    max_list_pages: int,
    max_empty_pages: int,
) -> List[dict]:
    collected = []

    for source in sources:
        if already_collected + len(collected) >= per_label:
            break

        source_name = source["name"]
        article_regex = source.get("article_regex")
        empty_pages = 0

        for page in range(1, max_list_pages + 1):
            if already_collected + len(collected) >= per_label:
                break

            listing_url = resolve_listing_page_url(source, page)
            print(f"\n[INFO] LISTING | nhãn='{label}' | nguồn={source_name} | page={page} | {listing_url}")

            candidate_links = collect_links_from_listing(listing_url, article_regex=article_regex)
            if not candidate_links:
                print(f"[WARN] Không lấy được link từ listing: {listing_url}")
                empty_pages += 1
                if empty_pages >= max_empty_pages:
                    print(f"[INFO] Dừng nguồn {source_name} vì {empty_pages} trang liên tiếp không lấy được link.")
                    break
                continue

            new_links = [u for u in candidate_links if u not in seen_urls]
            print(f"[INFO] Tìm thấy {len(candidate_links)} link, mới {len(new_links)} link")

            page_success = 0
            for link in new_links:
                if already_collected + len(collected) >= per_label:
                    break

                article = extract_article(url=link)
                time.sleep(sleep_sec)

                if article is None:
                    continue

                article["label"] = label
                article["source_feed"] = listing_url
                article["source_type"] = f"listing:{source_name}"
                article["published"] = ""

                path = save_article_json(out_dir, label, article)
                article["saved_file"] = str(path)

                seen_urls.add(link)
                collected.append(article)
                page_success += 1
                print(
                    f"[OK][LIST] {label}: {already_collected + len(collected)}/{per_label} | {article['title'][:90]}"
                )

            if page_success == 0 and len(new_links) == 0:
                empty_pages += 1
            else:
                empty_pages = 0

            if empty_pages >= max_empty_pages:
                print(f"[INFO] Dừng nguồn {source_name} vì {empty_pages} trang liên tiếp không có bài mới.")
                break

    return collected


def export_csv(all_articles: List[dict], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(all_articles)
    cols = [
        "label",
        "title",
        "sapo",
        "content",
        "url",
        "published",
        "source_feed",
        "source_type",
        "saved_file",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Đã lưu CSV: {csv_path}")


def parse_args():
    parser = argparse.ArgumentParser(description="Crawl dữ liệu báo tiếng Việt theo RSS + category listing")
    parser.add_argument("--out-dir", type=str, default="raw_news", help="Thư mục lưu JSON theo nhãn")
    parser.add_argument("--csv-path", type=str, default="data/dataset_raw.csv", help="File CSV tổng")
    parser.add_argument("--per-label", type=int, default=700, help="Số bài mục tiêu cho mỗi nhãn")
    parser.add_argument("--sleep", type=float, default=0.15, help="Thời gian nghỉ giữa các request tới bài viết")
    parser.add_argument("--max-list-pages", type=int, default=60, help="Số trang listing tối đa cho mỗi nguồn/nhãn")
    parser.add_argument(
        "--max-empty-pages",
        type=int,
        default=8,
        help="Số trang liên tiếp không có bài mới trước khi dừng 1 nguồn listing",
    )
    parser.add_argument("--only-label", type=str, default=None, help="Chỉ crawl 1 nhãn nếu cần")
    return parser.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    csv_path = Path(args.csv_path)

    all_articles: List[dict] = []

    labels = list(RSS_FEEDS.keys())
    if args.only_label:
        if args.only_label not in RSS_FEEDS:
            raise ValueError(f"Nhãn không hợp lệ: {args.only_label}")
        labels = [args.only_label]

    for label in labels:
        seen_urls: Set[str] = set()
        print("\n" + "=" * 80)
        print(f"BẮT ĐẦU CRAWL NHÃN: {label}")
        print("=" * 80)

        rss_articles = crawl_rss_sources(
            label=label,
            feed_urls=RSS_FEEDS.get(label, []),
            out_dir=out_dir,
            per_label=args.per_label,
            sleep_sec=args.sleep,
            seen_urls=seen_urls,
        )
        all_articles.extend(rss_articles)

        current_count = len(rss_articles)
        if current_count < args.per_label:
            listing_articles = crawl_listing_sources(
                label=label,
                sources=LISTING_SOURCES.get(label, []),
                out_dir=out_dir,
                per_label=args.per_label,
                sleep_sec=args.sleep,
                seen_urls=seen_urls,
                already_collected=current_count,
                max_list_pages=args.max_list_pages,
                max_empty_pages=args.max_empty_pages,
            )
            all_articles.extend(listing_articles)
            current_count += len(listing_articles)

        print(f"\n[SUMMARY] {label}: thu được {current_count} bài")

    export_csv(all_articles, csv_path)

    print("\n========== THỐNG KÊ ==========")
    if all_articles:
        df = pd.DataFrame(all_articles)
        print(df["label"].value_counts().sort_index())
        print(f"Tổng số bài: {len(df)}")
    else:
        print("Không thu được bài nào.")


if __name__ == "__main__":
    main()
