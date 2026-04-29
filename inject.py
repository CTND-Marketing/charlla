import csv, io, json, re, sys
from collections import defaultdict
from openpyxl import load_workbook
from datetime import datetime

def read_euckr(path):
    with open(path, 'rb') as f:
        return f.read().decode('euc-kr', errors='replace')

# config.json 읽기
try:
    with open('data/config.json', encoding='utf-8') as f:
        config = json.load(f)
except:
    config = {}

last_updated     = config.get('lastUpdated', '2026-04-20')
weekly_visitors  = config.get('weeklyVisitors', [None, None, None, None])
ad_costs         = config.get('adCosts', {'google':250,'gdn':100,'naver':210,'cafe24':369})

def map_visitor(g, s, m):
    g,s,m = g.strip().lower(), s.strip().lower(), m.strip().lower()
    if g == 'direct' or (s == '(direct)' and m == '(none)'): return ('direct','direct')
    if m in ('brand_search_pc','brand_search_m') or s == 'bs': return ('paid search','brand_Search')
    if g in ('paid search','paid video') and s == 'google' and m == 'cpc': return ('paid search','google')
    if g in ('paid search','paid shopping') and 'naver' in s: return ('paid search','naver')
    if s in ('ig','instagram','l.instagram.com'): return ('paid search','instagram')
    if g == 'organic search' and s == 'google': return ('organic search','google')
    if g == 'organic search' and ('naver' in s or s in ('m.naver.com','m.search.naver.com')): return ('organic search','naver')
    if g == 'organic search' and s == 'bing': return ('organic search','etc.(bing)')
    if s in ('inblog','blog','charlla_inblog','blog.naver.com','m.blog.naver.com','blog.catenoid.net'): return ('unassigned','blog')
    if g == 'organic social': return ('unassigned','blog')
    if s == 'iboss': return ('unassigned','iboss')
    if s == 'stibee' or m in ('ebook','ebook2'): return ('unassigned','stibee')
    if s == 'openads': return ('unassigned','openads')
    if s in ('catenoid.net','charlla.io'): return ('referral','catenoid.net / charlla.io')
    if s in ('chatgpt.com','gemini.google.com') or (g == 'referral' and s == 'ai'): return ('referral','ai')
    if g == 'referral' and any(x in s for x in ('cafe24','makeshop','sixshop')): return ('referral','cafe24 (ad)')
    if s in ('gdn','viimstudio','(data not available)') or (g == 'display' and s == 'google') or g == 'cross-network': return ('display','GDN banner')
    if 'cafe24' in s and m == 'floating_banner': return ('display','CAFE24 banner')
    if g == 'display' and ('cafe24' in s or m in ('banner','floating_banner')): return ('display','CAFE24 banner')
    if g == 'display': return ('display','etc.(GFA, DMP..)')
    if g == 'referral': return ('referral','etc.')
    if g == 'unassigned': return ('unassigned','etc.')
    return None

def map_event(s, m):
    s,m = s.strip().lower(), m.strip().lower()
    if s == '(direct)' or m == '(none)': return ('direct','direct')
    if m in ('brand_search_pc','brand_search_m') or s == 'bs': return ('paid search','brand_Search')
    if s == 'google' and m == 'cpc': return ('paid search','google')
    if s == 'naver' and m == 'cpc': return ('paid search','naver')
    if s in ('ig','instagram'): return ('paid search','instagram')
    if s == 'google' and m == 'organic': return ('organic search','google')
    if ('naver' in s or s == 'm.search.naver.com') and m in ('organic','referral'): return ('organic search','naver')
    if s == 'bing' and m == 'organic': return ('organic search','etc.(bing)')
    if s in ('blog.naver.com','m.blog.naver.com','inblog'): return ('unassigned','blog')
    if s in ('gdn','viimstudio','(data not available)'): return ('display','GDN banner')
    if 'cafe24' in s and m in ('referral','banner','floating_banner'): return ('referral','cafe24 (ad)')
    if s in ('chatgpt.com','gemini.google.com'): return ('referral','ai')
    if s in ('accounts.google.com','mail.google.com'): return ('direct','direct')
    if m == 'referral': return ('referral','etc.')
    if m in ('ebook','ebook2'): return ('unassigned','stibee')
    return None

stage_map = {
    'ga4-btn-home-freetrial':'ft','ga4-btn-gnb-freetrial':'ft','ga4-btn-basic-free':'ft',
    'ga4_btn_basic_free':'ft','ga4-btn-home-footer-freetrial':'ft','ga4-btn-lite-free':'ft',
    'ga4-btn-multiplayer-free':'ft','ga4-btn-playersetting-footer-free':'ft',
    'ga4-btn-playersetting-free':'ft','ga4-btn-playlist-footer-free':'ft',
    'ga4-btn-playlist-free':'ft','ga4-btn-playlist-free1':'ft','ga4-btn-playlist-free2':'ft',
    'ga4-btn-standard-free':'ft','ga4-btn-statistic-free':'ft','ga4-btn-statistic-footer-free':'ft',
    'sign_up_create_account':'ac','sign-up-create-account-social':'ac',
    'ga4-btn-policy-next':'po','ga4-btn-policy-social-next':'po',
    'sign_up':'su',
    'ga4-btn-shopinfo-next':None,'start_sign_up':None,
}

structure = [
    ('direct',         ['direct']),
    ('unassigned',     ['blog','outstanding','qletter','iboss','openads','stibee','etc.']),
    ('paid search',    ['brand_Search','google','naver','instagram']),
    ('organic search', ['google','naver','etc.(bing)']),
    ('referral',       ['catenoid.net / charlla.io','cafe24 (ad)','makeshop','ai','etc.']),
    ('display',        ['GDN banner','CAFE24 banner','IBOSS banner','Outstanding banner','etc.(GFA, DMP..)']),
    ('SNS',            ['facebook / linkedin']),
]

totals = {cat+'|'+row: {'v':0,'ft':0,'ac':0,'po':0,'su':0} for cat,rows in structure for row in rows}

# 유입 CSV
for row in list(csv.reader(io.StringIO(read_euckr('data/visitors.csv'))))[1:]:
    if len(row) < 4: continue
    try: n = int(row[3].strip())
    except: continue
    r = map_visitor(row[0],row[1],row[2])
    if r and r[0]+'|'+r[1] in totals: totals[r[0]+'|'+r[1]]['v'] += n

# 이벤트 CSV
for row in list(csv.reader(io.StringIO(read_euckr('data/events.csv'))))[1:]:
    if len(row) < 4: continue
    try: n = int(row[3].strip())
    except: continue
    st = stage_map.get(row[0].strip())
    if not st: continue
    r = map_event(row[1],row[2])
    if r and r[0]+'|'+r[1] in totals: totals[r[0]+'|'+r[1]][st] += n

# Metabase xlsx
monthSu = {1:0,2:0,3:0,4:0}
monthPaid = {1:0,2:0,3:0,4:0}
weekSu = {1:0,2:0,3:0}
try:
    wb = load_workbook('data/metabase.xlsx', read_only=True, data_only=True)
    ws = wb.active
    rows_mb = list(ws.iter_rows(values_only=True))
    headers = [str(h).strip() if h else '' for h in rows_mb[0]]
    join_idx = headers.index('가입일') if '가입일' in headers else None
    paid_idx = headers.index('유료전환 여부') if '유료전환 여부' in headers else None
    for row in rows_mb[1:]:
        if join_idx is None: continue
        raw = row[join_idx]
        if not raw: continue
        d = raw if isinstance(raw, datetime) else None
        if not d:
            try: d = datetime.fromisoformat(str(raw))
            except: continue
        mo = d.month
        if mo < 1 or mo > 4: continue
        monthSu[mo] += 1
        if paid_idx is not None and str(row[paid_idx]).strip().upper() == 'Y': monthPaid[mo] += 1
        if mo == 4:
            if d.day <= 8: weekSu[1] += 1
            elif d.day <= 15: weekSu[2] += 1
            else: weekSu[3] += 1
except Exception as e:
    print(f"Metabase 처리 오류: {e}")

# 집계
all_data = [(cat, row_name, totals[cat+'|'+row_name]) for cat, rows in structure for row_name in rows]
total_v  = sum(d['v']  for _,_,d in all_data)
total_ft = sum(d['ft'] for _,_,d in all_data)
total_ac = sum(d['ac'] for _,_,d in all_data)
total_po = sum(d['po'] for _,_,d in all_data)
total_su = sum(d['su'] for _,_,d in all_data)
ga4_cvr  = round(total_su/total_v*100, 2) if total_v else 0
mb4      = monthSu[4]
mb_cvr4  = round(mb4/total_v*100, 2) if total_v else 0

def cvr_type(v, su):
    if v == 0: return 'null'
    r = su/v*100
    if r >= 6: return 'high'
    if r >= 3: return 'mid'
    if r > 0: return 'low'
    return 'null'

td = {a+'|'+b: d for a,b,d in all_data}

# rawData JS
raw_lines = ['[']
for cat, rows in structure:
    raw_lines.append("  { cat: '" + cat + "', rows: [")
    for row_name in rows:
        d = td.get(cat+'|'+row_name, {'v':0,'ft':0,'ac':0,'po':0,'su':0})
        v,ft,ac,po,su = d['v'],d['ft'],d['ac'],d['po'],d['su']
        cvr_val = str(round(su/v*100,1))+'%' if v and su else ('0%' if v else '-')
        raw_lines.append("    { name: '"+row_name+"', v:"+str(v)+", ft:"+str(ft)+", ac:"+str(ac)+", po:"+str(po)+", su:"+str(su)+", cvr:'"+cvr_val+"', cvrType:'"+cvr_type(v,su)+"' },")
    raw_lines.append('  ]},')
raw_lines.append(']')

# 채널
ch_defs = [
    ('검색/배너광고','#3b82f6', lambda ct,n: ct=='paid search' or n=='GDN banner'),
    ('직접 유입',    '#10b981', lambda ct,n: ct=='direct'),
    ('카페24',       '#f59e0b', lambda ct,n: n in ('cafe24 (ad)','CAFE24 banner')),
    ('자연유입',     '#06b6d4', lambda ct,n: ct=='organic search' or n in ('blog','ai')),
    ('기타',         '#cbd5e1', lambda ct,n: (ct in ('unassigned','SNS')) or (ct=='referral' and n not in ('cafe24 (ad)',)) or (ct=='display' and n!='GDN banner')),
]
ch_data = []
for name, color, fn in ch_defs:
    rows = [(ct,n,d) for ct,n,d in all_data if fn(ct,n)]
    v  = sum(d['v']  for _,_,d in rows)
    su = sum(d['su'] for _,_,d in rows)
    ch_data.append({'name':name,'v':v,'su':su,'color':color})

# adEff - 광고비는 config.json에서 읽기 (만원 단위 → 원 변환)
ae_defs = [
    ("Google\nKeyword", 'paid search', ['google'],                    ad_costs.get('google',250)*10000, '#3b82f6'),
    ("GDN",             'display',     ['GDN banner'],                ad_costs.get('gdn',100)*10000,    '#7c3aed'),
    ("Naver\n검색광고", 'paid search', ['naver','brand_Search'],      ad_costs.get('naver',210)*10000,  '#10b981'),
    ("카페24\n배너",    None,          ['cafe24 (ad)','CAFE24 banner'],ad_costs.get('cafe24',369)*10000, '#f59e0b'),
]
adEff = []
for name, cat, rnames, cost, color in ae_defs:
    rows = [(ct,n,d) for ct,n,d in all_data if (cat is None or ct==cat) and n in rnames]
    v  = sum(d['v']  for _,_,d in rows)
    su = sum(d['su'] for _,_,d in rows)
    adEff.append({'name':name,'v':v,'su':su,'cost':cost,'color':color})

# HTML 읽기
with open('index_template.html', encoding='utf-8') as f:
    c = f.read()

# 주입
raw_start = c.find('const rawData = [')
raw_end   = c.find('const cvrColors', raw_start)
c = c[:raw_start] + 'const rawData = ' + '\n'.join(raw_lines) + ';\n' + c[raw_end:]

funnel = [total_v, total_ft, total_ac, total_po, total_su]
fd_start = c.find('const funnelData = [')
fd_end   = c.find(']', fd_start) + 1
c = c[:fd_start] + 'const funnelData = ' + json.dumps(funnel) + c[fd_end:]

ch_start = c.find('const channels = [')
ch_end   = c.find('const totalSU', ch_start)
ch_lines = ['const channels = [']
for d in ch_data:
    ch_lines.append("  { name:'"+d['name']+"', su:"+str(d['su'])+", v:"+str(d['v'])+", color:'"+d['color']+"' },")
ch_lines.append('];')
c = c[:ch_start] + '\n'.join(ch_lines) + '\n' + c[ch_end:]

tsu_s = c.find('const totalSU = ')
tsu_e = c.find(';', tsu_s) + 1
c = c[:tsu_s] + 'const totalSU = ' + str(total_su) + ';' + c[tsu_e:]

ae_str = 'const adEff = ['
for d in adEff:
    ae_str += "{name:'"+d['name'].replace('\n','\\n')+"',v:"+str(d['v'])+",su:"+str(d['su'])+",cost:"+str(d['cost'])+",color:'"+d['color']+"'},"
ae_str += '];'
ae_s = c.find('const adEff = [')
ae_e = c.find('];', ae_s) + 2
c = c[:ae_s] + ae_str + c[ae_e:]

def rep(html, id_, val):
    return re.sub('id="'+id_+'">[^<]*<', 'id="'+id_+'">'+str(val)+'<', html)

c = rep(c,'kpiTotalV', f'{total_v:,}')
c = rep(c,'kpiGA4Su',  str(total_su))
c = rep(c,'kpiGA4Cvr', str(ga4_cvr)+'%')
c = rep(c,'kpiMBSu',   str(mb4))
c = rep(c,'kpiMBCvr',  str(mb_cvr4)+'%')
c = rep(c,'accTotalV',  str(total_v))
c = rep(c,'accTotalFt', str(total_ft))
c = rep(c,'accTotalAc', str(total_ac))
c = rep(c,'accTotalPo', str(total_po))
c = rep(c,'accTotalSu', str(total_su))
c = rep(c,'accTotalCvr', str(ga4_cvr)+'%')

ad_keys = ['google','gdn','naver','cafe24']
for i, (k, cost) in enumerate(zip(ad_keys, [2500000,1000000,2100000,3690000])):
    v  = adEff[i]['v']
    su = adEff[i]['su']
    cvr = round(su/v*100,2) if v else 0
    cpa = round(cost/su/10000) if su else 0
    c = rep(c,'kpi_v_'+k,   str(v))
    c = rep(c,'kpi_su_'+k,  str(su))
    c = rep(c,'kpi_cvr_'+k, str(cvr)+'%')
    c = rep(c,'kpi_cpa_'+k, str(cpa)+'만')

def drop(a,b): return round((1-b/a)*100,1) if a else 0
c = re.sub(r'id="funnelDrop0">[^<]*<','id="funnelDrop0">'+str(drop(total_v,total_ft))+'<',c)
c = re.sub(r'id="funnelDrop1">[^<]*<','id="funnelDrop1">'+str(drop(total_ft,total_ac))+'<',c)
c = re.sub(r'id="funnelDrop2">[^<]*<','id="funnelDrop2">'+str(drop(total_ac,total_po))+'<',c)
c = re.sub(r'id="funnelDrop3">[^<]*<','id="funnelDrop3">'+str(drop(total_po,total_su))+'<',c)

# 주차별 방문자수 - config.json 우선, 없으면 자동 계산
wv = weekly_visitors + [None] * (4 - len(weekly_visitors))  # 4개 보장
w1 = wv[0] if wv[0] is not None else 0
w2 = wv[1] if wv[1] is not None else 0
w3 = wv[2] if wv[2] is not None else max(0, total_v - w1 - w2)
w4 = wv[3]  # null 허용
for wid, wval in [('wv1', w1), ('wv2', w2), ('wv3', w3), ('wv4', '' if w4 is None else w4)]:
    c = re.sub('id="'+wid+'" value="[^"]*"', 'id="'+wid+'" value="'+str(wval)+'"', c)

# 마지막 업데이트 날짜
c = re.sub('id="lastUpdated" value="[^"]*"', 'id="lastUpdated" value="'+last_updated+'"', c)
# 날짜 레이블
try:
    d = datetime.fromisoformat(last_updated)
    date_label = f'{d.month}/1 ~ {d.month}/{d.day} 기준'
except:
    date_label = '날짜 미설정'
c = re.sub('id="lastUpdatedLabel"[^>]*>[^<]*<', 'id="lastUpdatedLabel" class="text-xs text-gray-500 font-semibold">'+date_label+'<', c)

# 광고비 - config.json 값으로 input value 교체
for key in ['google', 'gdn', 'naver', 'cafe24']:
    cost_val = ad_costs.get(key, 0)
    c = re.sub('id="cost_'+key+'" [^>]*value="[^"]*"',
               lambda m, v=str(cost_val): m.group(0)[:m.group(0).find('value="')] + 'value="'+v+'"', c)

# 차트 데이터 script 주입
monthly_v       = [5046,4687,4532,total_v]
monthly_ga4     = [317,222,153,total_su]
monthly_mb      = [monthSu[1],monthSu[2],monthSu[3],mb4]
monthly_ga4_cvr = [6.28,4.74,3.38,ga4_cvr]
monthly_mb_cvr  = [6.46,5.23,4.37,mb_cvr4]
monthly_paid_mb = [monthSu[1],monthSu[2],monthSu[3],mb4]
monthly_paid    = [monthPaid[1],monthPaid[2],monthPaid[3],monthPaid[4]]
monthly_paid_r  = [round(monthPaid[k]/monthSu[k]*100,1) if monthSu[k] else 0 for k in [1,2,3,4]]
week_v   = [w1, w2, w3, w4]
week_mb  = [weekSu[1], weekSu[2], weekSu[3], None]
week_ga4 = [61, 49, max(0,total_su-61-49), None]

ch_su_pcts  = [round(d['su']/total_su*100,1) if total_su else 0 for d in ch_data]
ch_prev_pct = [[47,43,44],[22,25,29],[19,18,17],[9,12,7],[3,2,3]]
ch_prev_abs = [[149,96,87],[70,56,57],[60,40,34],[29,27,14],[9,4,6]]
cvrS = sorted(ch_data, key=lambda d: -(d['su']/d['v']) if d['v'] else 0)
# cvr 필드 추가 (JS 플러그인에서 d.cvr 참조)
for d in cvrS:
    d['cvr'] = round(d['su']/d['v']*100, 1) if d['v'] else 0

inits = 'window.addEventListener("load",function(){\n'
inits += 'if(window.weeklyAprilChartRef){var w=window.weeklyAprilChartRef;w.data.datasets[0].data='+json.dumps(week_v)+';w.data.datasets[1].data='+json.dumps(week_mb)+';w.data.datasets[2].data='+json.dumps(week_ga4)+';w.update();}\n'
inits += 'if(window.monthlyTopChartRef){var mt=window.monthlyTopChartRef;mt.data.datasets[0].data='+json.dumps(monthly_v)+';mt.data.datasets[1].data='+json.dumps(monthly_mb)+';mt.data.datasets[2].data='+json.dumps(monthly_ga4)+';mt.update();}\n'
inits += 'if(window.cvrTrendChartRef){var ct=window.cvrTrendChartRef;ct.data.datasets[0].data='+json.dumps(monthly_mb_cvr)+';ct.data.datasets[1].data='+json.dumps(monthly_ga4_cvr)+';ct.update();}\n'
inits += 'if(window.paidConvChartRef){var pc=window.paidConvChartRef;pc.data.datasets[0].data='+json.dumps(monthly_paid_mb)+';pc.data.datasets[1].data='+json.dumps(monthly_paid)+';pc.data.datasets[2].data='+json.dumps(monthly_paid_r)+';pc.update();}\n'
inits += 'if(window.cvrChartRef){var cv=window.cvrChartRef;cv.data.labels='+json.dumps([d['name'] for d in cvrS])+';cv.data.datasets[0].data='+json.dumps([round(d['su']/d['v']*100,1) if d['v'] else 0 for d in cvrS])+';cv.data.datasets[0].backgroundColor='+json.dumps([d['color']+'bb' for d in cvrS])+';cv.data.datasets[0].borderColor='+json.dumps([d['color'] for d in cvrS])+';cvrSorted.length=0;'+json.dumps(cvrS)+'.forEach(function(d){cvrSorted.push(d);});cv.update();}\n'
for i in range(len(ch_data)):
    inits += 'if(window.channelTrendChartRef){window.channelTrendChartRef.data.datasets['+str(i)+'].data='+json.dumps(ch_prev_pct[i]+[ch_su_pcts[i]])+';window.channelTrendChartRef.data.datasets['+str(i)+'].su='+json.dumps(ch_prev_abs[i]+[ch_data[i]['su']])+';}\n'
inits += 'if(window.channelTrendChartRef){window.channelTrendChartRef.update();}\n'

# adEff 차트 (CPA 바차트, 버블차트)
adEff_cpa   = [d['cost']//d['su'] if d['su'] else 0 for d in adEff]
adEff_cvr   = [round(d['su']/d['v']*100,2) if d['v'] else 0 for d in adEff]
adEff_cost  = [d['cost'] for d in adEff]
maxCost     = max(adEff_cost) if adEff_cost else 1
inits += 'if(typeof adCpaChartRef!=="undefined"&&adCpaChartRef){adCpaChartRef.data.datasets[0].data='+json.dumps(adEff_cpa)+';adCpaChartRef.update();}\n'
inits += 'if(typeof adEffChartRef!=="undefined"&&adEffChartRef){'+\
    ''.join(['adEffChartRef.data.datasets['+str(i)+'].data=[{x:'+str(adEff_cvr[i])+',y:'+str(round(adEff_cpa[i]/10000))+',r:'+str(max(8,round(adEff_cost[i]/maxCost*28)))+'}];' for i in range(len(adEff))])+\
    'adEffChartRef.update();}\n'
inits += '});\n'

c = c.rstrip()
if c.endswith('</body></html>'):
    c = c[:-len('</body></html>')] + '<script>\n' + inits + '</script>\n</body></html>'
else:
    c = c.replace('</html>', '<script>\n' + inits + '</script>\n</html>', 1)

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(c)

print(f"완료: 유입 {total_v:,}명 / GA4가입 {total_su}명 / MB가입 {mb4}명")
