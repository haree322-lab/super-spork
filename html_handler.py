import os
import re
import asyncio
import base64
from vars import CREDIT
from pyrogram import Client, filters
from pyrogram.types import Message
from pyromod import listen
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad


def aes_encrypt_auto_prefix(data: str) -> str:
    try:
        key = b'ThisIsASecretKey'
        cipher = AES.new(key, AES.MODE_CBC)
        ct_bytes = cipher.encrypt(pad(data.encode('utf-8'), AES.block_size))
        encrypted_data = base64.b64encode(cipher.iv + ct_bytes).decode('utf-8')
        return encrypted_data
    except Exception as e:
        return data


def get_player_url(url: str) -> str:
    if 'youtube.com' in url or 'youtu.be' in url:
        return url
    elif '.m3u8' in url or '.mpd' in url:
        return f'https://player.marshmallowapi.workers.dev/?video={url}'
    elif 'zip' in url:
        return f'https://video.pablocoder.eu.org/appx-zip?url={url}'
    elif 'brightcove' in url:
        bcov = 'bcov_auth=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3Mjg3MDIyMDYsImNvbiI6eyJpc0FkbWluIjpmYWxzZSwiYXVzZXIiOiJVMFZ6TkdGU2NuQlZjR3h5TkZwV09FYzBURGxOZHowOSIsImlkIjoiT0dweFpuWktabVl3WVdwRlExSXJhV013WVdvMlp6MDkiLCJmaXJzdF9uYW1lIjoiU0hCWVJFc3ZkbVJ0TVVSR1JqSk5WamN3VEdoYVp6MDkiLCJlbWFpbCI6ImNXbE5NRTVoTUd4NloxbFFORmx4UkhkWVV6bFhjelJTWWtwSlVVcHNSM0JDVTFKSWVGQXpRM2hsT0QwPSIsInBob25lIjoiYVhReWJ6TTJkWEJhYzNRM01uQjZibEZ4ZGxWR1p6MDkiLCJhdmF0YXIiOiJLM1ZzY1M4elMwcDBRbmxrYms4M1JEbHZla05pVVQwOSIsInJlZmVycmFsX2NvZGUiOiJla3RHYjJoYWRtcENXSFo0YTFsV2FEVlBaM042ZHowOSIsImRldmljZV90eXBlIjoiYW5kcm9pZCIsImRldmljZV92ZXJzaW9uIjoidXBwZXIgdGhhbiAzMSIsImRldmljZV9tb2RlbCI6IlhpYW9NaSBNMjAwN0oxN0MiLCJyZW1vdGVfYWRkciI6IjQ0LjIyMi4yNTMuODUifX0.k_419KObeIVpLO6BqHcg8MpnvEwDgm54UxPnY7rTUEu_SIjOaE7FOzez5NL9LS7LdI_GawTeibig3ILv5kWuHhDqAvXiM8sQpTkhQoGEYybx8JRFmPw_fyNsiwNxTZQ4P4RSF9DgN_yiQ61aFtYpcfldT0xG1AfamXK4JlneJpVOJ8aG_vOLm6WkiY-XG4PCj5u4C3iyur0VM1-j-EhwHmNXVCiCz5weXDsv6ccV6SqNW2j_Cbjia16ghgX61XeIyyEkp07Nyrp7GN4eXuxxHeKcoBJB-YsQ0OopSWKzOQNEjlGgx7b54BkmU8PbiwElYgMGpjRT9bLTf3EYnTJ_wA'
        return url.split("bcov_auth")[0] + bcov
    elif 'utkarsh' in url:
        return url
    else:
        return f'https://player.marshmallowapi.workers.dev/?video={url}'


def parse_line(line):
    line = line.strip()
    if ':' not in line: return None
    title_part, url = line.split(':', 1)
    title_part = title_part.strip()
    url = url.strip()

    subject = "General"
    category = None
    clean_title = title_part

    matches = re.findall(r'\[([^\]]+)\]|\(([^)]+)\)', title_part)
    if matches:
        subject = matches[0][0] if matches[0][0] else matches[0][1]
        subject = subject.strip()
        if len(matches) > 1:
            category = matches[1][0] if matches[1][0] else matches[1][1]
            category = category.strip()
        clean_title = re.sub(r'\[([^\]]+)\]|\(([^)]+)\)', '',
                             title_part).strip()

    is_pdf = '.pdf' in url.lower()
    is_image = any(ext in url.lower()
                   for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp'])

    return {
        "subject": subject,
        "category": category,
        "title": clean_title,
        "url": url,
        "is_pdf": is_pdf,
        "is_image": is_image
    }


COMMON_JS = """
<script>
    function showContent(tabName) {
        document.querySelectorAll('.content-section, .content').forEach(s=>s.classList.remove('active'));
        document.querySelectorAll('.nav-item, .tab').forEach(t=>t.classList.remove('active'));
        document.getElementById(tabName).classList.add('active');
        if(event && event.target) event.target.classList.add('active');
        var breadcrumb = document.querySelector('.breadcrumb span.active');
        if(breadcrumb) breadcrumb.textContent = tabName.charAt(0).toUpperCase() + tabName.slice(1);
    }

    function toggleVideos(subject) {
        var el = document.getElementById(subject);
        if(!el) return;
        var prev = el.previousElementSibling;
        var icon = prev ? prev.querySelector('.fa-chevron-down') : null;
        if(el.classList.toggle('active')){
            if(icon) icon.style.transform = 'rotate(180deg)';
        } else {
            if(icon) icon.style.transform = 'rotate(0deg)';
        }
    }

    function searchContent() {
        var input = document.getElementById('searchInput');
        var filter = input.value.toLowerCase();
        var items = document.querySelectorAll('.searchable-item');
        var subjects = document.querySelectorAll('.subject-card, .subject');

        subjects.forEach(sub => sub.style.display = '');

        items.forEach(function(item) {
            var text = item.textContent || item.innerText;
            if (text.toLowerCase().indexOf(filter) > -1) {
                item.style.display = "";
            } else {
                item.style.display = "none";
            }
        });

        subjects.forEach(function(subject) {
            var onclickAttr = subject.getAttribute('onclick');
            if(!onclickAttr) return;
            var match = onclickAttr.match(/'([^']+)'/);
            if(!match) return;
            var list = document.getElementById(match[1]);
            if(!list) return;
            var visibleItems = list.querySelectorAll('.searchable-item');
            var hasVisibleItems = false;

            visibleItems.forEach(function(i) {
                if(i.style.display !== 'none') hasVisibleItems = true;
            });

            if (filter !== "" && !hasVisibleItems && subject.textContent.toLowerCase().indexOf(filter) === -1) {
                subject.style.display = "none";
            } else if (filter === "") {
                subject.style.display = "";
            }
        });
    }

    function openVideoPopup(videoUrl, isDRM) {
        if (isDRM || videoUrl.includes('youtube.com') || videoUrl.includes('youtu.be')) {
            window.open(videoUrl, '_blank');
        } else {
            var popup = document.getElementById('videoPopup');
            var frame = document.getElementById('videoFrame');
            frame.src = videoUrl;
            popup.style.display = 'block';
            document.body.style.overflow = 'hidden';
        }
    }

    function closeVideoPopup(event) {
        if (event && event.target !== event.currentTarget) {
            return;
        }
        var popup = document.getElementById('videoPopup');
        var frame = document.getElementById('videoFrame');
        popup.style.display = 'none';
        frame.src = '';
        document.body.style.overflow = 'auto';
    }

    document.addEventListener('keydown', function(event) {
        if (event.key === 'Escape') {
            closeVideoPopup();
            closePdf();
        }
    });

    function openPdf(url) {
        document.getElementById('pdfModal').style.display = "flex";
        document.getElementById('pdfFrame').src = "https://mozilla.github.io/pdf.js/web/viewer.html?file=" + encodeURIComponent(url);
    }
    function closePdf() {
        document.getElementById('pdfModal').style.display = "none";
        document.getElementById('pdfFrame').src = "";
    }

    function toggleSidebar() {
        document.getElementById('sidebar').classList.toggle('active');
    }
</script>
"""

COMMON_VIDEO_POPUP = """
<div id="videoPopup" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.95);z-index:9999;" onclick="closeVideoPopup(event)">
    <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);background:#000;border-radius:8px;width:95%;max-width:1200px;height:85vh;max-height:800px;box-shadow:0 10px 40px rgba(0,0,0,0.8);overflow:hidden;" onclick="event.stopPropagation()">
        <button onclick="closeVideoPopup()" style="position:absolute;top:10px;left:10px;background:rgba(255,255,255,0.15);border:none;color:#fff;font-size:28px;cursor:pointer;border-radius:50%;width:44px;height:44px;display:flex;align-items:center;justify-content:center;z-index:10000;backdrop-filter:blur(10px);box-shadow:0 2px 10px rgba(0,0,0,0.3);">&times;</button>
        <iframe id="videoFrame" style="width:100%;height:100%;border:none;border-radius:8px;" allowfullscreen></iframe>
    </div>
</div>
"""

COMMON_PDF_MODAL = """
<div id="pdfModal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.95);z-index:9999;flex-direction:column;align-items:center;justify-content:center;">
    <button onclick="closePdf()" style="position:absolute;top:20px;right:20px;z-index:10000;background:#ef4444;color:white;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-weight:bold;">✕ Close PDF</button>
    <iframe id="pdfFrame" style="width:95%;height:95%;border:none;background:white;"></iframe>
</div>
"""


def _parse_file(input_file):
    video_links_by_subject = {}
    pdf_links = []
    image_links = []
    with open(input_file, 'r', encoding='utf-8') as file:
        for line in file:
            data = parse_line(line)
            if not data: continue
            if data['is_pdf']: pdf_links.append(data)
            elif data['is_image']: image_links.append(data)
            else:
                sub = data['subject']
                if sub not in video_links_by_subject:
                    video_links_by_subject[sub] = []
                video_links_by_subject[sub].append(data)
    return video_links_by_subject, pdf_links, image_links


async def extract_links_modern_dark(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)
    total_videos = sum(len(v) for v in video_links_by_subject.values())
    total_pdfs = len(pdf_links)
    total_images = len(image_links)

    html_content = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Mighty Atom Viewer</title><link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet"><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"><style>
        :root {{ --primary: #6366f1; --primary-dark: #4f46e5; --secondary: #ec4899; --bg-main: #0f172a; --bg-secondary: #1e293b; --bg-card: #1e293b; --text-primary: #f1f5f9; --text-secondary: #94a3b8; --border: #334155; --sidebar-width: 260px; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg-main); color: var(--text-primary); }}
        .sidebar {{ position: fixed; left: 0; top: 0; width: var(--sidebar-width); height: 100vh; background: var(--bg-secondary); border-right: 1px solid var(--border); padding: 20px; overflow-y: auto; transition: transform 0.3s ease; z-index: 100; }}
        .sidebar-header {{ margin-bottom: 30px; }}
        .logo {{ font-size: 1.5rem; font-weight: 800; color: white; text-decoration: none; display: flex; align-items: center; gap: 10px; }}
        .logo i {{ color: var(--primary); }}
        .stats-sidebar {{ display: flex; flex-direction: column; gap: 12px; margin-bottom: 25px; }}
        .stat-card {{ background: linear-gradient(135deg, var(--primary), var(--secondary)); padding: 15px; border-radius: 12px; display: flex; align-items: center; gap: 12px; }}
        .stat-icon {{ font-size: 1.8rem; opacity: 0.9; color: white; }}
        .stat-info {{ flex: 1; }}
        .stat-num {{ font-size: 1.5rem; font-weight: 700; color: white; }}
        .stat-label {{ font-size: 0.75rem; color: rgba(255,255,255,0.8); text-transform: uppercase; }}
        .menu {{ list-style: none; }}
        .menu-item {{ padding: 12px 15px; border-radius: 10px; margin-bottom: 5px; cursor: pointer; transition: all 0.3s; display: flex; align-items: center; gap: 12px; color: var(--text-secondary); }}
        .menu-item:hover, .menu-item.active {{ background: var(--bg-card); color: var(--primary); }}
        .main-content {{ margin-left: var(--sidebar-width); padding: 30px; min-height: 100vh; }}
        .top-bar {{ background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 15px; padding: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: center; gap: 20px; flex-wrap: wrap; }}
        .breadcrumb {{ display: flex; gap: 8px; align-items: center; color: var(--text-secondary); font-size: 1.1rem; font-weight: 600; }}
        .search-container {{ flex: 1; max-width: 400px; }}
        #searchInput {{ width: 100%; padding: 12px 45px 12px 20px; border: 1px solid var(--border); border-radius: 25px; background: var(--bg-card); color: var(--text-primary); outline: none; }}
        #searchInput:focus {{ border-color: var(--primary); }}
        .content-section {{ display: none; }}
        .content-section.active {{ display: block; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }}
        .subject-card {{ background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; cursor: pointer; transition: all 0.3s; display: flex; justify-content: space-between; align-items: center; }}
        .subject-card:hover {{ transform: translateY(-3px); border-color: var(--primary); }}
        .subject-title {{ font-size: 1.1rem; font-weight: 600; color: var(--text-primary); }}
        .subject-count {{ background: linear-gradient(135deg, var(--primary), var(--secondary)); color: white; border-radius: 20px; padding: 4px 12px; font-size: 0.8rem; font-weight: 600; }}
        .video-grid {{ display: none; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin-top: 20px; }}
        .video-grid.active {{ display: grid; }}
        .card {{ background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 15px; display: flex; align-items: center; gap: 15px; text-decoration: none; color: var(--text-primary); transition: all 0.3s; position: relative; }}
        .card:hover {{ border-color: var(--primary); transform: translateY(-3px); }}
        .card-icon {{ width: 36px; height: 36px; background: linear-gradient(135deg, var(--primary), var(--secondary)); border-radius: 8px; display: flex; align-items: center; justify-content: center; color: white; flex-shrink: 0; }}
        .card-content {{ flex: 1; overflow: hidden; }}
        .card-title {{ font-weight: 500; font-size: 0.95rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .card-meta {{ color: var(--text-secondary); font-size: 0.75rem; }}
        .play-btn {{ background: var(--primary); color: white; padding: 5px 10px; border-radius: 6px; font-size: 0.8rem; cursor: pointer; border: none; }}
        .pdf-btn {{ background: var(--secondary); color: white; padding: 5px 10px; border-radius: 6px; font-size: 0.8rem; cursor: pointer; border: none; }}
        .mobile-toggle {{ display: none; position: fixed; bottom: 20px; right: 20px; width: 50px; height: 50px; background: linear-gradient(135deg, var(--primary), var(--secondary)); border-radius: 50%; align-items: center; justify-content: center; color: white; font-size: 1.3rem; cursor: pointer; z-index: 101; }}
        @media (max-width: 968px) {{ .sidebar {{ transform: translateX(-100%); }} .sidebar.active {{ transform: translateX(0); }} .main-content {{ margin-left: 0; }} .mobile-toggle {{ display: flex; }} }}
    </style></head><body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><a href="#" class="logo"><i class="fas fa-code"></i> Mighty Atom</a></div>
        <div class="stats-sidebar">
            <div class="stat-card"><div class="stat-icon"><i class="fas fa-video"></i></div><div class="stat-info"><div class="stat-num">{total_videos}</div><div class="stat-label">Videos</div></div></div>
            <div class="stat-card"><div class="stat-icon"><i class="fas fa-file-pdf"></i></div><div class="stat-info"><div class="stat-num">{total_pdfs}</div><div class="stat-label">PDFs</div></div></div>
            <div class="stat-card"><div class="stat-icon"><i class="fas fa-image"></i></div><div class="stat-info"><div class="stat-num">{total_images}</div><div class="stat-label">Images</div></div></div>
        </div>
        <ul class="menu">
            <li class="menu-item active" onclick="showContent('videos')"><i class="fas fa-play"></i> Videos</li>
            <li class="menu-item" onclick="showContent('pdfs')"><i class="fas fa-file-pdf"></i> PDFs</li>
            <li class="menu-item" onclick="showContent('images')"><i class="fas fa-image"></i> Images</li>
        </ul>
    </div>
    <div class="mobile-toggle" onclick="toggleSidebar()"><i class="fas fa-bars"></i></div>
    <div class="main-content">
        <div class="top-bar">
            <div class="breadcrumb"><span class="active">Videos</span></div>
            <div class="search-container"><input type="text" id="searchInput" placeholder="Search content..." onkeyup="searchContent()"></div>
        </div>
        <section id="videos" class="content-section active"><div class="grid">"""
    for sub, vids in video_links_by_subject.items():
        html_content += f'<div class="subject-card" onclick="toggleVideos(\'{sub}\')"><div class="subject-title">{sub}</div><div class="subject-count"><span style="margin-right:5px">{len(vids)}</span><i class="fas fa-chevron-down" style="font-size:0.8rem;transition:0.3s"></i></div></div><div id="{sub}" class="video-grid">'
        for v in vids:
            p_url = get_player_url(v['url'])
            cat = f"• {v['category']}" if v['category'] else ""
            is_drm = '.mpd' in v['url'].lower()
            escaped_url = p_url.replace("'", "\\'")
            html_content += f'<div class="card searchable-item"><div class="card-icon"><i class="fas fa-play"></i></div><div class="card-content"><div class="card-title">{v["title"]}</div><div class="card-meta">Video {cat}</div></div><button class="play-btn" onclick="openVideoPopup(\'{escaped_url}\', {"true" if is_drm else "false"})">▶ Play</button></div>'
        html_content += '</div>'
    html_content += """</div></section>
        <section id="pdfs" class="content-section"><div class="grid">"""
    for p in pdf_links:
        if 'selectionwayserver.hranker.com' in p["url"]:
            html_content += f'<a href="{p["url"]}" target="_blank" class="card searchable-item"><div class="card-icon"><i class="fas fa-file-pdf"></i></div><div class="card-content"><div class="card-title">{p["title"]}</div><div class="card-meta">PDF Document</div></div><span class="pdf-btn">View</span></a>'
        else:
            html_content += f'<div class="card searchable-item"><div class="card-icon"><i class="fas fa-file-pdf"></i></div><div class="card-content"><div class="card-title">{p["title"]}</div><div class="card-meta">PDF Document</div></div><button class="pdf-btn" onclick="openPdf(\'{p["url"]}\')">View</button></div>'
    html_content += """</div></section>
        <section id="images" class="content-section"><div class="grid">"""
    for i in image_links:
        html_content += f'<a href="{i["url"]}" target="_blank" class="card searchable-item"><div class="card-icon"><i class="fas fa-image"></i></div><div class="card-content"><div class="card-title">{i["title"]}</div><div class="card-meta">Image</div></div></a>'
    html_content += f"""</div></section>
    </div>
    {COMMON_VIDEO_POPUP}{COMMON_PDF_MODAL}{COMMON_JS}
    </body></html>"""
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


async def extract_links_neumorphic(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)
    total_videos = sum(len(v) for v in video_links_by_subject.values())

    html_content = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Mighty Atom</title><link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet"><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"><style>
        :root {{ --bg: #e0e5ec; --card: #e0e5ec; --text: #4a5568; --accent: #6c5ce7; --shadow-light: #ffffff; --shadow-dark: #a3b1c6; }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: 'Inter', sans-serif; }}
        body {{ background: var(--bg); color: var(--text); padding: 20px; min-height: 100vh; }}
        .container {{ max-width: 1100px; margin: 0 auto; }}
        .header {{ text-align: center; margin-bottom: 30px; }}
        h1 {{ font-size: 2.5rem; color: var(--accent); }}
        #searchInput {{ width: 100%; max-width: 400px; padding: 15px 25px; border-radius: 50px; border: none; outline: none; background: var(--card); box-shadow: 6px 6px 12px var(--shadow-dark), -6px -6px 12px var(--shadow-light); color: var(--text); margin-bottom: 20px; }}
        .tabs {{ display: flex; justify-content: center; gap: 20px; margin-bottom: 30px; }}
        .tab {{ padding: 10px 25px; border-radius: 15px; cursor: pointer; font-weight: 600; background: var(--card); box-shadow: 5px 5px 10px var(--shadow-dark), -5px -5px 10px var(--shadow-light); transition: 0.3s; }}
        .tab.active {{ box-shadow: inset 5px 5px 10px var(--shadow-dark), inset -5px -5px 10px var(--shadow-light); color: var(--accent); }}
        .content {{ display: none; }}
        .content.active {{ display: block; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 25px; }}
        .subject {{ padding: 20px; border-radius: 20px; cursor: pointer; margin-bottom: 20px; background: var(--card); box-shadow: 8px 8px 16px var(--shadow-dark), -8px -8px 16px var(--shadow-light); }}
        .video-list {{ display: none; grid-column: 1 / -1; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }}
        .video-list.active {{ display: grid; }}
        .card {{ padding: 20px; border-radius: 15px; background: var(--card); box-shadow: 6px 6px 12px var(--shadow-dark), -6px -6px 12px var(--shadow-light); transition: 0.3s; display: flex; align-items: center; gap: 15px; text-decoration: none; color: inherit; }}
        .card:hover {{ box-shadow: inset 4px 4px 8px var(--shadow-dark), inset -4px -4px 8px var(--shadow-light); }}
        .card i {{ font-size: 1.5rem; color: var(--accent); }}
        .pdf-btn {{ margin-left: auto; padding: 5px 10px; background: var(--accent); color: white; border: none; border-radius: 10px; cursor: pointer; }}
    </style></head><body><div class="container">
        <div class="header"><h1>Mighty Atom</h1><input type="text" id="searchInput" placeholder="🔍 Search..." onkeyup="searchContent()"></div>
        <div class="tabs"><div class="tab active" onclick="showContent('videos')">Videos</div><div class="tab" onclick="showContent('pdfs')">PDFs</div><div class="tab" onclick="showContent('images')">Images</div></div>
        <div id="videos" class="content active"><div class="grid">"""
    for sub, vids in video_links_by_subject.items():
        html_content += f'<div class="subject" onclick="toggleVideos(\'{sub}\')"><h3>{sub}</h3></div><div id="{sub}" class="video-list">'
        for v in vids:
            p_url = get_player_url(v['url'])
            is_drm = '.mpd' in v['url'].lower()
            escaped_url = p_url.replace("'", "\\'")
            html_content += f'<div class="card searchable-item" style="cursor:pointer;" onclick="openVideoPopup(\'{escaped_url}\', {"true" if is_drm else "false"})"><i class="fas fa-play-circle"></i><span>{v["title"]}</span></div>'
        html_content += '</div>'
    html_content += """</div></div><div id="pdfs" class="content"><div class="grid">"""
    for p in pdf_links:
        if 'selectionwayserver.hranker.com' in p["url"]:
            html_content += f'<a href="{p["url"]}" target="_blank" class="card searchable-item"><i class="fas fa-file-pdf"></i><span>{p["title"]}</span><span class="pdf-btn">View</span></a>'
        else:
            html_content += f'<div class="card searchable-item"><i class="fas fa-file-pdf"></i><span>{p["title"]}</span><button class="pdf-btn" onclick="openPdf(\'{p["url"]}\')">View</button></div>'
    html_content += """</div></div><div id="images" class="content"><div class="grid">"""
    for i in image_links:
        html_content += f'<a href="{i["url"]}" target="_blank" class="card searchable-item"><i class="fas fa-image"></i><span>{i["title"]}</span></a>'
    html_content += f"""</div></div></div>{COMMON_VIDEO_POPUP}{COMMON_PDF_MODAL}{COMMON_JS}</body></html>"""
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


async def extract_links_brutalist(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)

    html_content = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>BRUTALIST</title><link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap" rel="stylesheet"><style>
        *{{margin:0;padding:0;box-sizing:border-box;font-family:'Space Mono',monospace;}}
        body{{background:#000;color:#fff;padding:20px;}}
        .container{{max-width:1100px;margin:0 auto;border:5px solid #fff;padding:20px;}}
        h1{{font-size:3rem;text-transform:uppercase;letter-spacing:-2px;margin-bottom:20px;text-align:center;}}
        #searchInput{{width:100%;background:#000;border:2px solid #fff;color:#fff;padding:15px;font-size:1.2rem;margin-bottom:20px;font-family:'Space Mono';}}
        .tabs{{display:flex;gap:10px;margin-bottom:30px;flex-wrap:wrap;}}
        .tab{{background:#fff;color:#000;padding:10px 20px;border:2px solid #fff;cursor:pointer;font-weight:bold;text-transform:uppercase;}}
        .tab.active{{background:#000;color:#fff;}}
        .content{{display:none;}}
        .content.active{{display:block;}}
        .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;}}
        .card{{background:#fff;color:#000;padding:20px;border:2px solid #fff;text-decoration:none;display:block;}}
        .subject{{background:transparent;border:3px solid #fff;padding:20px;margin-bottom:20px;cursor:pointer;text-transform:uppercase;font-size:1.5rem;font-weight:bold;}}
        .video-list{{display:none;grid-column:1/-1;}}
        .video-list.active{{display:grid;}}
        .pdf-btn{{display:block;margin-top:10px;background:#000;color:#fff;padding:5px;border:2px solid #fff;cursor:pointer;text-align:center;}}
    </style></head><body><div class="container">
        <h1>BRUTALIST // DATA</h1>
        <input type="text" id="searchInput" placeholder="SEARCH_DATA..." onkeyup="searchContent()">
        <div class="tabs"><div class="tab active" onclick="showContent('videos')">VIDEOS</div><div class="tab" onclick="showContent('pdfs')">PDFS</div><div class="tab" onclick="showContent('images')">IMAGES</div></div>
        <div id="videos" class="content active"><div class="grid">"""
    for sub, vids in video_links_by_subject.items():
        html_content += f'<div class="subject" onclick="toggleVideos(\'{sub}\')">{sub}</div><div id="{sub}" class="video-list">'
        for v in vids:
            p_url = get_player_url(v['url'])
            is_drm = '.mpd' in v['url'].lower()
            escaped_url = p_url.replace("'", "\\'")
            html_content += f'<div class="card searchable-item" style="cursor:pointer;" onclick="openVideoPopup(\'{escaped_url}\', {"true" if is_drm else "false"})">> {v["title"]}</div>'
        html_content += '</div>'
    html_content += """</div></div><div id="pdfs" class="content"><div class="grid">"""
    for p in pdf_links:
        if 'selectionwayserver.hranker.com' in p["url"]:
            html_content += f'<a href="{p["url"]}" target="_blank" class="card searchable-item">> {p["title"]}</a>'
        else:
            html_content += f'<div class="card searchable-item">> {p["title"]}<div class="pdf-btn" onclick="openPdf(\'{p["url"]}\')">[ VIEW PDF ]</div></div>'
    html_content += """</div></div><div id="images" class="content"><div class="grid">"""
    for i in image_links:
        html_content += f'<a href="{i["url"]}" target="_blank" class="card searchable-item">> {i["title"]}</a>'
    html_content += f"""</div></div></div>{COMMON_VIDEO_POPUP}{COMMON_PDF_MODAL}{COMMON_JS}</body></html>"""
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


async def extract_links_glassmorphism(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)

    html_content = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Glass</title><link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;500;600&display=swap" rel="stylesheet"><style>
        body{{margin:0;padding:0;background:linear-gradient(45deg,#1a1a2e,#16213e);background-attachment:fixed;color:#fff;font-family:'Poppins',sans-serif;min-height:100vh;}}
        .container{{max-width:1100px;margin:0 auto;padding:20px;}}
        .glass{{background:rgba(255,255,255,0.05);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.1);border-radius:15px;}}
        h1{{text-align:center;margin-bottom:20px;}}
        #searchInput{{width:100%;padding:15px;border-radius:30px;background:rgba(255,255,255,0.1);border:none;color:#fff;margin-bottom:20px;outline:none;}}
        .tabs{{display:flex;justify-content:center;gap:15px;margin-bottom:30px;}}
        .tab{{padding:10px 25px;border-radius:20px;cursor:pointer;background:rgba(255,255,255,0.1);transition:0.3s;}}
        .tab.active{{background:#ff0055;}}
        .content{{display:none;}}
        .content.active{{display:block;}}
        .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;}}
        .card{{padding:20px;text-decoration:none;color:#fff;display:block;transition:0.3s;}}
        .card:hover{{background:rgba(255,255,255,0.1);}}
        .subject{{padding:20px;cursor:pointer;margin-bottom:15px;font-weight:bold;}}
        .video-list{{display:none;grid-column:1/-1;}}
        .video-list.active{{display:grid;}}
        .pdf-btn{{float:right;color:#ff0055;font-weight:bold;cursor:pointer;border:none;background:none;}}
    </style></head><body><div class="container">
        <div class="glass" style="padding:20px;margin-bottom:20px;"><h1>Glass View</h1></div>
        <div class="glass" style="padding:15px;margin-bottom:20px;"><input type="text" id="searchInput" placeholder="🔍 Search..." onkeyup="searchContent()"></div>
        <div class="tabs"><div class="tab active" onclick="showContent('videos')">Videos</div><div class="tab" onclick="showContent('pdfs')">PDFs</div><div class="tab" onclick="showContent('images')">Images</div></div>
        <div id="videos" class="content active"><div class="grid">"""
    for sub, vids in video_links_by_subject.items():
        html_content += f'<div class="glass subject" onclick="toggleVideos(\'{sub}\')">{sub}</div><div id="{sub}" class="video-list">'
        for v in vids:
            p_url = get_player_url(v['url'])
            is_drm = '.mpd' in v['url'].lower()
            escaped_url = p_url.replace("'", "\\'")
            html_content += f'<div class="glass card searchable-item" style="cursor:pointer;" onclick="openVideoPopup(\'{escaped_url}\', {"true" if is_drm else "false"})">▶ {v["title"]}</div>'
        html_content += '</div>'
    html_content += """</div></div><div id="pdfs" class="content"><div class="grid">"""
    for p in pdf_links:
        if 'selectionwayserver.hranker.com' in p["url"]:
            html_content += f'<a href="{p["url"]}" target="_blank" class="glass card searchable-item">📄 {p["title"]}<span class="pdf-btn">View PDF</span></a>'
        else:
            html_content += f'<div class="glass card searchable-item">📄 {p["title"]}<button class="pdf-btn" onclick="openPdf(\'{p["url"]}\')">View PDF</button></div>'
    html_content += """</div></div><div id="images" class="content"><div class="grid">"""
    for i in image_links:
        html_content += f'<a href="{i["url"]}" target="_blank" class="glass card searchable-item">🖼️ {i["title"]}</a>'
    html_content += f"""</div></div></div>{COMMON_VIDEO_POPUP}{COMMON_PDF_MODAL}{COMMON_JS}</body></html>"""
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


async def extract_links_cyberpunk(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)

    html_content = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>CYBERPUNK</title><link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700&display=swap" rel="stylesheet"><style>
        body{{background:#050505;color:#00ff41;font-family:'Orbitron',sans-serif;padding:20px;}}
        .container{{max-width:1100px;margin:0 auto;border:2px solid #00ff41;box-shadow:0 0 10px #00ff41;padding:20px;}}
        h1{{text-align:center;color:#00ff41;text-shadow:0 0 5px #00ff41;}}
        #searchInput{{width:100%;background:#000;border:2px solid #00ff41;color:#00ff41;padding:15px;margin-bottom:20px;font-family:'Orbitron';}}
        .tabs{{display:flex;gap:20px;margin-bottom:30px;}}
        .tab{{border:2px solid #00ff41;padding:10px 20px;cursor:pointer;transition:0.3s;}}
        .tab.active{{background:#00ff41;color:#000;}}
        .content{{display:none;}}
        .content.active{{display:block;}}
        .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;}}
        .card{{border:1px solid #00ff41;padding:15px;text-decoration:none;color:#00ff41;display:block;transition:0.3s;}}
        .card:hover{{background:rgba(0,255,65,0.1);}}
        .subject{{font-size:1.2rem;margin-bottom:20px;padding:10px;border-bottom:1px solid #00ff41;cursor:pointer;}}
        .video-list{{display:none;grid-column:1/-1;}}
        .video-list.active{{display:grid;}}
        .pdf-btn{{display:block;margin-top:10px;color:#ff0055;border:1px solid #ff0055;padding:5px;text-align:center;cursor:pointer;}}
    </style></head><body><div class="container">
        <h1>CYBER_VAULT</h1>
        <input type="text" id="searchInput" placeholder="SEARCH_SYSTEM..." onkeyup="searchContent()">
        <div class="tabs"><div class="tab active" onclick="showContent('videos')">VIDEOS</div><div class="tab" onclick="showContent('pdfs')">PDFS</div><div class="tab" onclick="showContent('images')">IMAGES</div></div>
        <div id="videos" class="content active"><div class="grid">"""
    for sub, vids in video_links_by_subject.items():
        html_content += f'<div class="subject" onclick="toggleVideos(\'{sub}\')">> {sub}</div><div id="{sub}" class="video-list">'
        for v in vids:
            p_url = get_player_url(v['url'])
            is_drm = '.mpd' in v['url'].lower()
            escaped_url = p_url.replace("'", "\\'")
            html_content += f'<div class="card searchable-item" style="cursor:pointer;" onclick="openVideoPopup(\'{escaped_url}\', {"true" if is_drm else "false"})">> {v["title"]}</div>'
        html_content += '</div>'
    html_content += """</div></div><div id="pdfs" class="content"><div class="grid">"""
    for p in pdf_links:
        if 'selectionwayserver.hranker.com' in p["url"]:
            html_content += f'<a href="{p["url"]}" target="_blank" class="card searchable-item">> {p["title"]}</a>'
        else:
            html_content += f'<div class="card searchable-item">> {p["title"]}<div class="pdf-btn" onclick="openPdf(\'{p["url"]}\')">[VIEW_DOC]</div></div>'
    html_content += """</div></div><div id="images" class="content"><div class="grid">"""
    for i in image_links:
        html_content += f'<a href="{i["url"]}" target="_blank" class="card searchable-item">> {i["title"]}</a>'
    html_content += f"""</div></div></div>{COMMON_VIDEO_POPUP}{COMMON_PDF_MODAL}{COMMON_JS}</body></html>"""
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


async def extract_links_mellow(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)
    total_videos = sum(len(v) for v in video_links_by_subject.values())
    total_pdfs = len(pdf_links)
    total_images = len(image_links)
    total_links = total_videos + total_pdfs + total_images

    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d at %H:%M:%S")
    base_name = os.path.splitext(os.path.basename(output_file))[0]
    if '_' in base_name:
        base_name = base_name.rsplit('_', 1)[0]

    html_content = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>{base_name}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
  <style>
    :root {{
      --bg-color: #121212;
      --text-color: #e8e8e8;
      --primary-color: #1e88e5;
      --secondary-color: #26a69a;
      --success-color: #43a047;
      --warning-color: #ff9800;
      --card-bg: #1e1e1e;
      --section-bg: #2a2a2a;
      --link-color: #42a5f5;
      --pdf-color: #ff7043;
      --image-color: #26c6da;
      --video-color: #ff3d71;
      --button-text: #ffffff;
      --gradient-primary: linear-gradient(135deg, #1e88e5 0%, #1976d2 100%);
      --gradient-secondary: linear-gradient(135deg, #26a69a 0%, #00695c 100%);
      --gradient-thumbnail: linear-gradient(135deg, #26c6da 0%, #0097a7 100%);
      --gradient-video: linear-gradient(135deg, #ff3d71 0%, #c2185b 100%);
      --shadow-glow: 0 4px 20px rgba(30, 136, 229, 0.25);
      --border-color: #333333;
    }}

    [data-theme="light"] {{
      --bg-color: #f8f9ff;
      --text-color: #2d3748;
      --primary-color: #0066cc;
      --secondary-color: #e67e22;
      --success-color: #27ae60;
      --warning-color: #f39c12;
      --card-bg: #ffffff;
      --section-bg: #e9ecef;
      --link-color: #0066cc;
      --pdf-color: #e67e22;
      --image-color: #16a085;
      --video-color: #e91e63;
      --button-text: #ffffff;
      --gradient-primary: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
      --gradient-secondary: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
      --gradient-thumbnail: linear-gradient(135deg, #16a085 0%, #f4d03f 100%);
      --gradient-video: linear-gradient(135deg, #e91e63 0%, #ad1457 100%);
      --shadow-glow: 0 4px 20px rgba(0, 102, 204, 0.2);
      --border-color: #e2e8f0;
    }}

    * {{
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }}

    body {{
      font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
      background: var(--bg-color);
      color: var(--text-color);
      padding: 16px;
      margin: 0;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
      line-height: 1.5;
      min-height: 100vh;
    }}

    h1 {{
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      text-align: center;
      font-size: clamp(1.6rem, 4vw, 2.5rem);
      font-weight: 800;
      margin: 12px 0 6px 0;
      letter-spacing: -0.5px;
    }}

    .conversion-info {{
      text-align: center;
      margin: 4px 0 12px 0;
      font-size: 0.85rem;
      opacity: 0.8;
    }}

    .meta-info {{
      background: var(--card-bg);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 14px;
      margin: 8px auto 20px auto;
      max-width: 380px;
      text-align: center;
      color: var(--text-color);
      font-size: 0.95rem;
      font-weight: 600;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    }}

    .total-links {{
      background: var(--gradient-primary);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      font-weight: 700;
      font-size: 1.1rem;
    }}

    .section-button {{
      display: block;
      width: 100%;
      padding: 14px 20px;
      margin: 14px 0;
      font-size: 1rem;
      font-weight: 600;
      text-align: center;
      color: var(--button-text);
      background: var(--gradient-primary);
      border: none;
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
      box-shadow: var(--shadow-glow);
      position: relative;
      overflow: hidden;
    }}

    .thumbnail-button {{
      background: var(--gradient-thumbnail);
      box-shadow: 0 4px 20px rgba(38, 198, 218, 0.25);
    }}

    .section-button::before {{
      content: '';
      position: absolute;
      top: 0;
      left: -100%;
      width: 100%;
      height: 100%;
      background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
      transition: left 0.5s ease;
    }}

    .section-button:hover {{
      transform: translateY(-3px) scale(1.02);
      box-shadow: 0 6px 25px rgba(30, 136, 229, 0.4);
    }}

    .thumbnail-button:hover {{
      box-shadow: 0 6px 25px rgba(38, 198, 218, 0.4);
    }}

    .section-button:hover::before {{
      left: 100%;
    }}

    .section {{
      display: none;
      margin-bottom: 20px;
      animation: fadeInUp 0.4s ease;
    }}

    @keyframes fadeInUp {{
      from {{
        opacity: 0;
        transform: translateY(20px);
      }}
      to {{
        opacity: 1;
        transform: translateY(0);
      }}
    }}

    .topic-button {{
      background: var(--gradient-secondary);
      font-size: 0.95rem;
      padding: 11px 18px;
      box-shadow: 0 4px 20px rgba(38, 166, 154, 0.25);
    }}

    .topic-button:hover {{
      box-shadow: 0 6px 25px rgba(38, 166, 154, 0.4);
    }}

    .video-play-button {{
      background: var(--gradient-video);
      color: var(--button-text);
      border: none;
      padding: 8px 16px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.85rem;
      font-weight: 600;
      margin-left: 8px;
      transition: all 0.3s ease;
      box-shadow: 0 2px 8px rgba(255, 61, 113, 0.3);
    }}

    .video-play-button:hover {{
      transform: translateY(-2px);
      box-shadow: 0 4px 15px rgba(255, 61, 113, 0.5);
    }}

    .youtube-play-button {{
      background: linear-gradient(135deg, #ff0000 0%, #cc0000 100%);
      color: var(--button-text);
      border: none;
      padding: 8px 16px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.85rem;
      font-weight: 600;
      margin-left: 8px;
      transition: all 0.3s ease;
      box-shadow: 0 2px 8px rgba(255, 0, 0, 0.3);
    }}

    .youtube-play-button:hover {{
      transform: translateY(-2px);
      box-shadow: 0 4px 15px rgba(255, 0, 0, 0.5);
    }}

    .popup-overlay {{
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0, 0, 0, 0.95);
      z-index: 1000;
    }}

    .popup-content {{
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      background: #000000;
      border-radius: 8px;
      width: 95%;
      max-width: 1200px;
      height: 85vh;
      max-height: 800px;
      box-shadow: 0 10px 40px rgba(0, 0, 0, 0.8);
      overflow: hidden;
    }}

    .close-button {{
      position: absolute;
      top: 10px;
      left: 10px;
      background: rgba(255, 255, 255, 0.15);
      border: none;
      color: #ffffff;
      font-size: 28px;
      cursor: pointer;
      padding: 8px 14px;
      border-radius: 50%;
      width: 44px;
      height: 44px;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: all 0.3s ease;
      z-index: 1000;
      backdrop-filter: blur(10px);
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
    }}

    .close-button:hover {{
      background: rgba(255, 255, 255, 0.25);
      transform: scale(1.1);
    }}

    .video-container {{
      width: 100%;
      height: 100%;
      padding: 0;
    }}

    .video-frame {{
      width: 100%;
      height: 100%;
      border: none;
      border-radius: 8px;
    }}

    ul {{
      list-style-type: none;
      padding: 0;
      margin: 0;
    }}

    li {{
      background: var(--card-bg);
      margin: 10px 0;
      padding: 14px 16px;
      border-radius: 10px;
      box-shadow: 0 3px 12px rgba(0, 0, 0, 0.15);
      font-size: 0.95rem;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      border: 1px solid var(--border-color);
    }}

    li:hover {{
      transform: translateY(-3px) scale(1.01);
      box-shadow: 0 6px 20px rgba(0, 0, 0, 0.25);
      border-color: var(--primary-color);
      background: var(--section-bg);
    }}

    .number {{
      font-weight: 700;
      color: var(--secondary-color);
      margin-right: 10px;
      min-width: 26px;
      font-size: 1rem;
    }}

    .link-title {{
      color: var(--link-color);
      text-decoration: none;
      flex-grow: 1;
      word-break: break-word;
      font-weight: 500;
      font-size: 0.95rem;
      transition: all 0.3s ease;
    }}

    .link-title:hover {{
      text-decoration: underline;
      color: var(--primary-color);
    }}

    .pdf-title {{
      color: var(--pdf-color);
    }}

    .image-title {{
      color: var(--image-color);
    }}

    .video-title {{
      color: var(--video-color);
    }}

    .link-controls {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin-left: 8px;
    }}

    .controls {{
      display: flex;
      justify-content: center;
      gap: 10px;
      margin-bottom: 25px;
      flex-wrap: wrap;
    }}

    .theme-toggle {{
      padding: 10px 20px;
      background: var(--gradient-primary);
      border: none;
      border-radius: 8px;
      cursor: pointer;
      color: var(--button-text);
      font-size: 0.95rem;
      font-weight: 600;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
      box-shadow: var(--shadow-glow);
    }}

    .theme-toggle:hover {{
      transform: translateY(-2px) scale(1.05);
      box-shadow: 0 6px 20px rgba(30, 136, 229, 0.4);
    }}

    @media (max-width: 768px) {{
      body {{
        padding: 12px;
      }}
      .controls {{
        flex-direction: column;
        align-items: stretch;
      }}
      .theme-toggle {{
        width: 100%;
        margin-bottom: 10px;
      }}
      li {{
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
      }}
      .number {{
        margin-bottom: 4px;
      }}
      .link-controls {{
        margin-left: 0;
        margin-top: 8px;
      }}
      .popup-content {{
        width: 98%;
        height: 90vh;
      }}
      .close-button {{
        top: 5px;
        right: 5px;
        width: 35px;
        height: 35px;
        font-size: 20px;
      }}
    }}
  </style>
</head>
<body>
<h1>{base_name}</h1>
<div class="conversion-info">
  <div><i class="fas fa-magic"></i> Converted by: {CREDIT}</div>
  <div><i class="fas fa-clock"></i> {now}</div>
</div>
<div class="meta-info">
  <div class="total-links">
    <i class="fas fa-link"></i> Total links: {total_links}
  </div>
</div>

<div class="controls">
  <button class="theme-toggle" onclick="toggleTheme()"><i class="fas fa-palette"></i> Toggle Theme</button>
</div>"""

    if image_links:
        images_by_subject = {}
        for img in image_links:
            sub = img.get('subject', 'General') or 'General'
            if sub not in images_by_subject:
                images_by_subject[sub] = []
            images_by_subject[sub].append(img)
        html_content += f"""<button class='section-button thumbnail-button' onclick="toggleSection('thumbnails')">  <i class="fas fa-image"></i> Thumbnails ({len(image_links)})</button><div id='thumbnails' class='section'>"""
        for sub, imgs in images_by_subject.items():
            safe_sub = sub.replace(' ', '_').replace("'", "\\'")
            html_content += f"""<button class='section-button topic-button' onclick="toggleSection('thumbnail_topic_{safe_sub}')">  <i class="fas fa-folder"></i> {sub} ({len(imgs)})</button><div id='thumbnail_topic_{safe_sub}' class='section'><ul>"""
            for idx, img in enumerate(imgs, 1):
                html_content += f"""<li>  <span class='number'>{idx}.</span>  <a href='{img["url"]}' class='link-title image-title' target='_blank'>{img["title"]}</a></li>"""
            html_content += """</ul></div>"""
        html_content += """</div>"""

    if video_links_by_subject:
        html_content += f"""<button class='section-button' onclick="toggleSection('classes')">  <i class="fas fa-video"></i> Classes ({total_videos})</button><div id='classes' class='section'>"""
        for sub, vids in video_links_by_subject.items():
            safe_sub = sub.replace(' ', '_').replace("'", "\\'")
            html_content += f"""<button class='section-button topic-button' onclick="toggleSection('class_topic_{safe_sub}')">  <i class="fas fa-folder"></i> {sub} ({len(vids)})</button><div id='class_topic_{safe_sub}' class='section'><ul>"""
            for idx, v in enumerate(vids, 1):
                p_url = get_player_url(v['url'])
                is_drm = '.mpd' in v['url'].lower()
                is_youtube = 'youtube.com' in v['url'] or 'youtu.be' in v['url']
                escaped_url = p_url.replace("'", "\\'")
                html_content += f"""<li>  <span class='number'>{idx}.</span>  <a href="javascript:void(0)" class='link-title video-title' onclick="openVideoPopup('{escaped_url}', {'true' if is_drm else 'false'})">{v["title"]}</a>  <div class='link-controls'>    <button class='{"youtube-play-button" if is_youtube else "video-play-button"}' onclick="openVideoPopup('{escaped_url}', {'true' if is_drm else 'false'})">      <i class="fas fa-play"></i> Play    </button>    <a href='{v["url"]}' target='_blank' style='margin-left: 8px; color: var(--link-color); text-decoration: none; font-size: 0.8rem;'>Original</a>  </div></li>"""
            html_content += """</ul></div>"""
        html_content += """</div>"""

    if pdf_links:
        pdfs_by_subject = {}
        for p in pdf_links:
            sub = p.get('subject', 'General') or 'General'
            if sub not in pdfs_by_subject:
                pdfs_by_subject[sub] = []
            pdfs_by_subject[sub].append(p)
        html_content += f"""<button class='section-button' onclick="toggleSection('notes')">  <i class="fas fa-file-pdf"></i> Notes ({total_pdfs})</button><div id='notes' class='section'>"""
        for sub, pdfs in pdfs_by_subject.items():
            safe_sub = sub.replace(' ', '_').replace("'", "\\'")
            html_content += f"""<button class='section-button topic-button' onclick="toggleSection('note_topic_{safe_sub}')">  <i class="fas fa-folder"></i> {sub} ({len(pdfs)})</button><div id='note_topic_{safe_sub}' class='section'><ul>"""
            for idx, p in enumerate(pdfs, 1):
                html_content += f"""<li>  <span class='number'>{idx}.</span>  <a href='{p["url"]}' class='link-title pdf-title' target='_blank'>{p["title"]}</a></li>"""
            html_content += """</ul></div>"""
        html_content += """</div>"""

    html_content += f"""
<div id="videoPopup" class="popup-overlay" onclick="closeVideoPopup(event)">
  <div class="popup-content" onclick="event.stopPropagation()">
    <button class="close-button" onclick="closeVideoPopup()">&times;</button>
    <div class="video-container">
      <iframe id="videoFrame" class="video-frame" src="" allowfullscreen></iframe>
    </div>
  </div>
</div>

<script>
  document.addEventListener('DOMContentLoaded', function() {{
    const savedTheme = localStorage.getItem('theme') || 'dark';
    if (savedTheme === 'light') {{
      document.body.setAttribute('data-theme', 'light');
    }}
  }});

  function toggleSection(id) {{
    const section = document.getElementById(id);
    if (section.style.display === 'block') {{
      section.style.display = 'none';
    }} else {{
      section.style.display = 'block';
    }}
  }}

  function toggleTheme() {{
    const body = document.body;
    const currentTheme = body.getAttribute('data-theme');
    if (currentTheme === 'light') {{
      body.removeAttribute('data-theme');
      localStorage.setItem('theme', 'dark');
    }} else {{
      body.setAttribute('data-theme', 'light');
      localStorage.setItem('theme', 'light');
    }}
  }}

  function openVideoPopup(videoUrl, isDRM) {{
    if (isDRM) {{
      window.open(videoUrl, '_blank');
    }} else {{
      const popup = document.getElementById('videoPopup');
      const frame = document.getElementById('videoFrame');
      frame.src = videoUrl;
      popup.style.display = 'block';
      document.body.style.overflow = 'hidden';
    }}
  }}

  function closeVideoPopup(event) {{
    if (event && event.target !== event.currentTarget) {{
      return;
    }}
    const popup = document.getElementById('videoPopup');
    const frame = document.getElementById('videoFrame');
    popup.style.display = 'none';
    frame.src = '';
    document.body.style.overflow = 'auto';
  }}

  document.addEventListener('keydown', function(event) {{
    if (event.key === 'Escape') {{
      closeVideoPopup();
    }}
  }});
</script>
</body>
</html>"""
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


async def extract_links_yengo(input_file, output_file):
    video_links_by_subject, pdf_links, image_links = _parse_file(input_file)
    total_videos = sum(len(v) for v in video_links_by_subject.values())
    total_pdfs = len(pdf_links)
    total_images = len(image_links)
    total_subjects = len(video_links_by_subject)

    import json as _json

    course_data = {}
    for sub, vids in video_links_by_subject.items():
        if sub not in course_data:
            course_data[sub] = {}
        for v in vids:
            cat = v.get('category') or 'All Files'
            if cat not in course_data[sub]:
                course_data[sub][cat] = []
            p_url = get_player_url(v['url'])
            is_drm = '.mpd' in v['url'].lower()
            is_yt = 'youtu' in v['url'].lower()
            domain = 'youtube' if is_yt else 'drm' if is_drm else 'video'
            course_data[sub][cat].append({
                "name": v['title'],
                "url": v['url'],
                "player_url": p_url,
                "type": "video",
                "domain": domain,
                "is_drm": is_drm
            })

    if pdf_links:
        pdf_sub = "PDF Documents"
        course_data[pdf_sub] = {"All Files": []}
        for p in pdf_links:
            course_data[pdf_sub]["All Files"].append({
                "name": p['title'],
                "url": p['url'],
                "player_url": "",
                "type": "pdf",
                "domain": "pdf",
                "is_drm": False
            })

    if image_links:
        img_sub = "Images"
        course_data[img_sub] = {"All Files": []}
        for i in image_links:
            course_data[img_sub]["All Files"].append({
                "name": i['title'],
                "url": i['url'],
                "player_url": "",
                "type": "image",
                "domain": "image",
                "is_drm": False
            })

    course_data_json = _json.dumps(course_data, ensure_ascii=False)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yengo Viewer</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css">
    <style>
        :root {{
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --secondary: #ec4899;
            --bg-main: #0f172a;
            --bg-secondary: #1e293b;
            --bg-card: #1e293b;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --border: #334155;
            --sidebar-width: 260px;
        }}

        .light-mode {{
            --bg-main: #f8fafc;
            --bg-secondary: #ffffff;
            --bg-card: #ffffff;
            --text-primary: #0f172a;
            --text-secondary: #64748b;
            --border: #e2e8f0;
        }}

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-main);
            color: var(--text-primary);
            transition: all 0.3s ease;
        }}

        .sidebar {{
            position: fixed;
            left: 0;
            top: 0;
            width: var(--sidebar-width);
            height: 100vh;
            background: var(--bg-secondary);
            border-right: 1px solid var(--border);
            padding: 20px;
            overflow-y: auto;
            transition: transform 0.3s ease;
            z-index: 100;
        }}

        .sidebar-header {{ margin-bottom: 30px; }}

        .logo {{
            font-size: 1.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-decoration: none;
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
            cursor: pointer;
        }}

        .logo i {{
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .theme-toggle {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 25px;
            padding: 8px 15px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
            color: var(--text-secondary);
            font-size: 0.85rem;
            transition: all 0.3s;
            margin-top: 10px;
        }}

        .theme-toggle:hover {{ border-color: var(--primary); color: var(--primary); }}

        .stats-sidebar {{
            display: flex;
            flex-direction: column;
            gap: 12px;
            margin-bottom: 25px;
        }}

        .stat-card {{
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            padding: 15px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            gap: 12px;
        }}

        .stat-icon {{ font-size: 1.8rem; opacity: 0.9; }}
        .stat-info {{ flex: 1; }}
        .stat-num {{ font-size: 1.5rem; font-weight: 700; color: white; }}
        .stat-label {{ font-size: 0.75rem; color: rgba(255,255,255,0.8); text-transform: uppercase; }}

        .menu {{ list-style: none; }}

        .menu-item {{
            padding: 12px 15px;
            border-radius: 10px;
            margin-bottom: 5px;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            gap: 12px;
            color: var(--text-secondary);
        }}

        .menu-item:hover, .menu-item.active {{ background: var(--bg-card); color: var(--primary); }}
        .menu-item i {{ width: 20px; text-align: center; }}

        .main-content {{
            margin-left: var(--sidebar-width);
            padding: 20px;
            min-height: 100vh;
        }}

        .top-bar {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 15px;
            padding: 20px;
            margin-bottom: 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 20px;
            flex-wrap: wrap;
        }}

        .breadcrumb {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
        .breadcrumb span {{ color: var(--text-secondary); cursor: pointer; transition: color 0.3s; font-size: 0.9rem; }}
        .breadcrumb span:hover {{ color: var(--primary); }}
        .breadcrumb .active {{ color: var(--primary); font-weight: 600; }}

        .search-container {{ flex: 1; max-width: 400px; position: relative; }}

        .search-box {{
            width: 100%;
            padding: 12px 45px 12px 20px;
            border: 1px solid var(--border);
            border-radius: 25px;
            background: var(--bg-card);
            color: var(--text-primary);
            font-size: 0.95rem;
            transition: all 0.3s;
            outline: none;
        }}

        .search-box:focus {{ border-color: var(--primary); box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1); }}
        .search-icon {{ position: absolute; right: 18px; top: 50%; transform: translateY(-50%); color: var(--text-secondary); }}

        .view {{ display: none; }}
        .view.active {{ display: block; }}

        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 12px;
        }}

        .grid.folder-grid {{ grid-template-columns: 1fr; }}

        .card {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 15px;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            gap: 12px;
            overflow: visible;
            position: relative;
        }}

        .card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(99, 102, 241, 0.2);
            border-color: var(--primary);
        }}

        .card-header {{ display: flex; align-items: center; gap: 12px; }}
        .card-icon {{ font-size: 1.8rem; flex-shrink: 0; display: flex; align-items: center; justify-content: center; }}
        .folder-icon {{ color: #f59e0b; }}
        .video-icon {{ color: #ef4444; }}
        .pdf-icon {{ color: #dc2626; }}
        .youtube-icon {{ color: #ff0000; }}
        .image-icon {{ color: #10b981; }}

        .card-content {{ flex: 1; min-width: 0; }}

        .card-title {{
            font-size: 0.95rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 4px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: 100%;
            word-break: break-word;
        }}

        .card-meta {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}

        .tooltip-wrapper {{ position: relative; width: 100%; overflow: visible; }}

        .tooltip {{
            position: absolute;
            bottom: calc(100% + 12px);
            left: 50%;
            transform: translateX(-50%) translateY(8px) scale(0.95);
            background: var(--bg-secondary);
            color: var(--text-primary);
            padding: 10px 14px;
            border-radius: 10px;
            font-size: 0.85rem;
            width: max-content;
            max-width: min(320px, 90vw);
            word-wrap: break-word;
            white-space: normal;
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.25);
            border: 1px solid var(--border);
            opacity: 0;
            pointer-events: none;
            transition: all 0.25s cubic-bezier(0.34, 1.56, 0.64, 1);
            z-index: 1000;
            line-height: 1.5;
            text-align: center;
            backdrop-filter: blur(10px);
        }}

        .tooltip::after {{
            content: '';
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            border: 8px solid transparent;
            border-top-color: var(--bg-secondary);
        }}

        .card:hover .tooltip {{ opacity: 1; transform: translateX(-50%) translateY(0) scale(1); pointer-events: auto; }}

        .empty-state {{
            text-align: center;
            padding: 60px 20px;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 15px;
        }}

        .empty-state i {{ font-size: 3.5rem; color: var(--text-secondary); margin-bottom: 15px; opacity: 0.5; }}
        .empty-state h3 {{ font-size: 1.3rem; color: var(--text-primary); margin-bottom: 8px; }}
        .empty-state p {{ color: var(--text-secondary); }}

        .highlight {{
            background: rgba(99, 102, 241, 0.2);
            padding: 2px 4px;
            border-radius: 3px;
            color: var(--primary);
        }}

        .mobile-toggle {{
            display: none;
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 50px;
            height: 50px;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            border-radius: 50%;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 1.3rem;
            cursor: pointer;
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.4);
            z-index: 101;
        }}

        @media (max-width: 968px) {{
            .sidebar {{ transform: translateX(-100%); }}
            .sidebar.active {{ transform: translateX(0); }}
            .main-content {{ margin-left: 0; padding: 15px; }}
            .mobile-toggle {{ display: flex; }}
            .grid {{ grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; }}
            .top-bar {{ flex-direction: column; align-items: stretch; }}
            .search-container {{ max-width: 100%; }}
            .card-title {{
                white-space: normal; word-wrap: break-word; word-break: break-word;
                display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical;
                overflow: hidden; text-overflow: ellipsis; line-height: 1.4; max-height: 4.2em;
            }}
            .card-meta {{ white-space: normal; word-wrap: break-word; margin-top: 4px; }}
            .tooltip {{ display: none !important; }}
            .card {{ min-height: auto; padding: 12px; }}
            .card-content {{ overflow: visible; }}
        }}

        @media (max-width: 480px) {{
            .grid {{ grid-template-columns: 1fr; gap: 10px; }}
            .card-title {{ -webkit-line-clamp: 4; max-height: 5.6em; font-size: 0.9rem; }}
            .card-icon {{ font-size: 1.5rem; }}
        }}

        ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
        ::-webkit-scrollbar-track {{ background: var(--bg-main); }}
        ::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 4px; }}
        ::-webkit-scrollbar-thumb:hover {{ background: var(--text-secondary); }}
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header">
            <div class="logo" onclick="goHome()">
                <i class="fas fa-bolt"></i>
                <span>Yengo</span>
            </div>
            <div class="theme-toggle" onclick="toggleTheme()">
                <i class="fas fa-moon" id="themeIcon"></i>
                <span id="themeText">Dark Mode</span>
            </div>
        </div>

        <div class="stats-sidebar">
            <div class="stat-card">
                <div class="stat-icon">📚</div>
                <div class="stat-info">
                    <div class="stat-num">{total_subjects}</div>
                    <div class="stat-label">Subjects</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">🎥</div>
                <div class="stat-info">
                    <div class="stat-num">{total_videos}</div>
                    <div class="stat-label">Videos</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">📄</div>
                <div class="stat-info">
                    <div class="stat-num">{total_pdfs}</div>
                    <div class="stat-label">PDFs</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">🖼️</div>
                <div class="stat-info">
                    <div class="stat-num">{total_images}</div>
                    <div class="stat-label">Images</div>
                </div>
            </div>
        </div>

        <ul class="menu">
            <li class="menu-item active" onclick="goHome()">
                <i class="fas fa-home"></i>
                <span>Home</span>
            </li>
            <li class="menu-item" onclick="document.getElementById('searchInput').focus()">
                <i class="fas fa-search"></i>
                <span>Search</span>
            </li>
        </ul>
    </div>

    <div class="mobile-toggle" onclick="toggleSidebar()">
        <i class="fas fa-bars"></i>
    </div>

    <div class="main-content">
        <div class="top-bar">
            <div class="breadcrumb" id="breadcrumb">
                <span class="active" onclick="goHome()">
                    <i class="fas fa-home"></i> Home
                </span>
            </div>
            <div class="search-container">
                <input type="text" class="search-box" placeholder="Search videos, PDFs, images..." id="searchInput">
                <i class="fas fa-search search-icon"></i>
            </div>
        </div>

        <div class="view active" id="mainView">
            <div class="grid" id="mainGrid"></div>
        </div>
        <div class="view" id="folderView">
            <div class="grid" id="folderGrid"></div>
        </div>
        <div class="view" id="subfolderView">
            <div class="grid" id="subfolderGrid"></div>
        </div>
        <div class="view" id="searchView">
            <div class="grid" id="searchGrid"></div>
        </div>
    </div>

    {COMMON_VIDEO_POPUP}
    {COMMON_PDF_MODAL}

<script>
    const courseData = {course_data_json};
    let currentPath = [];
    let searchTimeout;

    function initTheme() {{
        const now = new Date();
        const istOffset = 5.5 * 60 * 60 * 1000;
        const istTime = new Date(now.getTime() + istOffset);
        const hour = istTime.getUTCHours();
        if (hour >= 20 || hour < 6) {{
            document.body.classList.remove('light-mode');
            updateThemeUI(true);
        }} else {{
            document.body.classList.add('light-mode');
            updateThemeUI(false);
        }}
    }}

    function toggleTheme() {{
        document.body.classList.toggle('light-mode');
        const isDark = !document.body.classList.contains('light-mode');
        updateThemeUI(isDark);
    }}

    function updateThemeUI(isDark) {{
        document.getElementById('themeIcon').className = isDark ? 'fas fa-moon' : 'fas fa-sun';
        document.getElementById('themeText').textContent = isDark ? 'Dark Mode' : 'Light Mode';
    }}

    function toggleSidebar() {{
        document.getElementById('sidebar').classList.toggle('active');
    }}

    function init() {{
        initTheme();
        renderMainView();
        setupSearch();
    }}

    function renderMainView() {{
        const grid = document.getElementById('mainGrid');
        grid.innerHTML = '';
        Object.keys(courseData).forEach(subject => {{
            const totalItems = Object.values(courseData[subject]).reduce((sum, arr) => sum + arr.length, 0);
            const card = document.createElement('div');
            card.className = 'card';
            card.onclick = () => openFolder(subject);
            card.innerHTML = `
                <div class="card-header">
                    <div class="card-icon folder-icon"><i class="fas fa-folder"></i></div>
                    <div class="card-content">
                        <div class="tooltip-wrapper">
                            <div class="card-title">${{subject}}</div>
                            ${{subject.length > 30 ? `<div class="tooltip">${{subject}}</div>` : ''}}
                        </div>
                        <div class="card-meta">${{totalItems}} items</div>
                    </div>
                </div>`;
            grid.appendChild(card);
        }});
    }}

    function openFolder(subject) {{
        currentPath = [subject];
        updateBreadcrumb();
        updateMenuActive(false);
        const grid = document.getElementById('folderGrid');
        grid.innerHTML = '';
        const categories = Object.keys(courseData[subject]);
        if (categories.length === 1 && categories[0] === 'All Files') {{
            openSubfolder(subject, 'All Files');
            return;
        }}
        categories.forEach(category => {{
            const items = courseData[subject][category];
            const card = document.createElement('div');
            card.className = 'card';
            card.onclick = () => openSubfolder(subject, category);
            card.innerHTML = `
                <div class="card-header">
                    <div class="card-icon folder-icon"><i class="fas fa-folder-open"></i></div>
                    <div class="card-content">
                        <div class="tooltip-wrapper">
                            <div class="card-title">${{category}}</div>
                            ${{category.length > 30 ? `<div class="tooltip">${{category}}</div>` : ''}}
                        </div>
                        <div class="card-meta">${{items.length}} files</div>
                    </div>
                </div>`;
            grid.appendChild(card);
        }});
        showView('folderView');
    }}

    function openSubfolder(subject, category) {{
        currentPath = [subject, category];
        updateBreadcrumb();
        const items = courseData[subject][category];
        const grid = document.getElementById('subfolderGrid');
        grid.innerHTML = '';
        for (let i = 0; i < items.length; i++) {{
            const item = items[i];
            const card = document.createElement('div');
            card.className = 'card';
            card.onclick = () => openFile(item);
            let iconClass = 'video-icon';
            let iconName = 'fa-play-circle';
            let iconPrefix = 'fas';
            if (item.type === 'pdf') {{
                iconClass = 'pdf-icon';
                iconName = 'fa-file-pdf';
            }} else if (item.type === 'image') {{
                iconClass = 'image-icon';
                iconName = 'fa-image';
            }} else if (item.domain === 'youtube') {{
                iconClass = 'youtube-icon';
                iconName = 'fa-youtube';
                iconPrefix = 'fab';
            }}
            card.innerHTML = `
                <div class="card-header">
                    <div class="card-icon ${{iconClass}}"><i class="${{iconPrefix}} ${{iconName}}"></i></div>
                    <div class="card-content">
                        <div class="tooltip-wrapper">
                            <div class="card-title">${{item.name}}</div>
                            ${{item.name.length > 30 ? `<div class="tooltip">${{item.name}}</div>` : ''}}
                        </div>
                        <div class="card-meta">${{item.type}}</div>
                    </div>
                </div>`;
            grid.appendChild(card);
        }}
        showView('subfolderView');
    }}

    function openFile(item) {{
        if (item.type === 'pdf') {{
            if (item.url.includes('selectionwayserver.hranker.com')) {{
                window.open(item.url, '_blank');
            }} else {{
                openPdf(item.url);
            }}
        }} else if (item.type === 'image') {{
            window.open(item.url, '_blank');
        }} else if (item.is_drm || item.domain === 'youtube') {{
            window.open(item.url, '_blank');
        }} else {{
            openVideoPopup(item.player_url, false);
        }}
    }}

    function showView(viewId) {{
        document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
        document.getElementById(viewId).classList.add('active');
    }}

    function updateBreadcrumb() {{
        const breadcrumb = document.getElementById('breadcrumb');
        breadcrumb.innerHTML = '<span onclick="goHome()"><i class="fas fa-home"></i> Home</span>';
        currentPath.forEach((item, index) => {{
            breadcrumb.innerHTML += `<span style="color: var(--text-secondary);">›</span>
                <span class="${{index === currentPath.length - 1 ? 'active' : ''}}" onclick="navigateTo(${{index}})">${{item}}</span>`;
        }});
    }}

    function navigateTo(index) {{
        if (index === 0) openFolder(currentPath[0]);
        else if (index === 1) openSubfolder(currentPath[0], currentPath[1]);
    }}

    function goHome() {{
        currentPath = [];
        document.getElementById('breadcrumb').innerHTML = '<span class="active" onclick="goHome()"><i class="fas fa-home"></i> Home</span>';
        showView('mainView');
        updateMenuActive(true);
    }}

    function updateMenuActive(isHome) {{
        document.querySelectorAll('.menu-item').forEach(item => item.classList.remove('active'));
        if (isHome) document.querySelector('.menu-item').classList.add('active');
    }}

    function setupSearch() {{
        const searchInput = document.getElementById('searchInput');
        searchInput.addEventListener('input', function() {{
            clearTimeout(searchTimeout);
            const query = this.value.trim();
            if (!query) {{
                if (currentPath.length === 0) showView('mainView');
                else if (currentPath.length === 1) showView('folderView');
                else showView('subfolderView');
                return;
            }}
            searchTimeout = setTimeout(() => performSearch(query), 300);
        }});
    }}

    function performSearch(query) {{
        const results = [];
        const queryLower = query.toLowerCase();
        Object.keys(courseData).forEach(subject => {{
            Object.keys(courseData[subject]).forEach(category => {{
                courseData[subject][category].forEach(item => {{
                    if (item.name.toLowerCase().includes(queryLower)) {{
                        results.push({{...item, subject, category}});
                    }}
                }});
            }});
        }});
        displaySearchResults(results, query);
    }}

    function displaySearchResults(results, query) {{
        const grid = document.getElementById('searchGrid');
        grid.innerHTML = '';
        if (results.length === 0) {{
            grid.innerHTML = '<div class="empty-state"><i class="fas fa-search"></i><h3>No Results Found</h3><p>Try different keywords</p></div>';
        }} else {{
            results.forEach(item => {{
                const card = document.createElement('div');
                card.className = 'card';
                card.onclick = () => openFile(item);
                let iconClass = 'video-icon';
                let iconName = 'fa-play-circle';
                let iconPrefix = 'fas';
                if (item.type === 'pdf') {{ iconClass = 'pdf-icon'; iconName = 'fa-file-pdf'; }}
                else if (item.type === 'image') {{ iconClass = 'image-icon'; iconName = 'fa-image'; }}
                else if (item.domain === 'youtube') {{ iconClass = 'youtube-icon'; iconName = 'fa-youtube'; iconPrefix = 'fab'; }}
                const highlightedName = item.name.replace(new RegExp(query, 'gi'), match => `<span class="highlight">${{match}}</span>`);
                card.innerHTML = `
                    <div class="card-header">
                        <div class="card-icon ${{iconClass}}"><i class="${{iconPrefix}} ${{iconName}}"></i></div>
                        <div class="card-content">
                            <div class="card-title">${{highlightedName}}</div>
                            <div class="card-meta">${{item.subject}} › ${{item.category}}</div>
                        </div>
                    </div>`;
                grid.appendChild(card);
            }});
        }}
        showView('searchView');
    }}

    function openVideoPopup(videoUrl, isDRM) {{
        if (isDRM) {{
            window.open(videoUrl, '_blank');
        }} else {{
            var popup = document.getElementById('videoPopup');
            var frame = document.getElementById('videoFrame');
            frame.src = videoUrl;
            popup.style.display = 'block';
            document.body.style.overflow = 'hidden';
        }}
    }}

    function closeVideoPopup(event) {{
        if (event && event.target !== event.currentTarget) return;
        var popup = document.getElementById('videoPopup');
        var frame = document.getElementById('videoFrame');
        popup.style.display = 'none';
        frame.src = '';
        document.body.style.overflow = 'auto';
    }}

    function openPdf(url) {{
        document.getElementById('pdfModal').style.display = 'flex';
        document.getElementById('pdfFrame').src = 'https://mozilla.github.io/pdf.js/web/viewer.html?file=' + encodeURIComponent(url);
    }}

    function closePdf() {{
        document.getElementById('pdfModal').style.display = 'none';
        document.getElementById('pdfFrame').src = '';
    }}

    document.addEventListener('keydown', function(event) {{
        if (event.key === 'Escape') {{
            closeVideoPopup();
            closePdf();
        }}
    }});

    init();
</script>
</body>
</html>"""

    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(html_content)


THEME_MAP = {
    "modern": extract_links_modern_dark,
    "neumorphic": extract_links_neumorphic,
    "brutalist": extract_links_brutalist,
    "glassmorphism": extract_links_glassmorphism,
    "cyberpunk": extract_links_cyberpunk,
    "mellow": extract_links_mellow,
    "yengo": extract_links_yengo,
}


async def html_handler(bot: Client, message: Message):
    await message.reply_text(f"🎐 **Welcome {message.from_user.first_name}!**\n"
                             "✨ **TXT ➝ HTML Bot** ✨\n"
                             "📌 **Features:**\n"
                             "• Pro Sidebar Layout\n"
                             "• Fixed Grid & Index\n"
                             "• In-App Player (No new tab)\n"
                             "• Smart Search\n"
                             "━━━━━━━━━━━━━━━━━━\n"
                             "🎨 **Themes:**\n"
                             "🔓 /modern → Pro Sidebar (Fixed)\n"
                             "🔓 /neumorphic → Soft Grey\n"
                             "🔓 /brutalist → Bold & Raw\n"
                             "🔓 /glassmorphism → Glass Effect\n"
                             "🔓 /cyberpunk → Neon Tech\n"
                             "🔓 /mellow → Accordion Toggle\n"
                             "🔓 /yengo → Folder Explorer\n"
                             "━━━━━━━━━━━━━━━━━━\n"
                             f"👑 By: {CREDIT}")


async def process_txt_to_html(bot: Client, message: Message, theme: str):
    user_id = message.from_user.id
    await message.reply(
        f"🕹️ **Generating `{theme}`...**\n📤 Please send `.txt` file.")
    try:
        msg: Message = await bot.listen(user_id, timeout=300)
    except asyncio.TimeoutError:
        await message.reply("⏰ Timeout!")
        return

    if not msg.document or not msg.document.file_name.endswith(".txt"):
        await msg.reply("❌ Only `.txt` files allowed.")
        return

    file_path = await msg.download()
    original_name = msg.document.file_name.replace(".txt", "")
    output_path = f"{original_name}_{user_id}.html"
    await msg.reply("⏳ Processing...")

    try:
        await THEME_MAP[theme](file_path, output_path)
        await msg.reply_document(document=output_path,
                                 file_name=f"{original_name}.html",
                                 caption=f"✅ Theme: `{theme}` | By {CREDIT}")
    except Exception as e:
        await msg.reply(f"❌ Error: `{str(e)}`")
    finally:
        for f in [file_path, output_path]:
            if os.path.exists(f): os.remove(f)
