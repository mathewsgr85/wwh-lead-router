import os,json,logging,requests
from datetime import datetime
from flask import Flask,request,jsonify

SMS_AGENT_URL=os.environ.get('SMS_AGENT_URL','')
EMAIL_AGENT_URL=os.environ.get('EMAIL_AGENT_URL','')
FB_VERIFY_TOKEN=os.environ.get('FB_VERIFY_TOKEN','wwh_fb_2024')

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)
app=Flask(__name__)
seen_leads={}

def clean_phone(p):
    d=''.join(c for c in str(p) if c.isdigit())
    if d.startswith('1') and len(d)==11: d=d[1:]
    return d

def score_lead(lead):
    s=0
    if lead.get('phone'): s+=25
    if lead.get('email'): s+=20
    if lead.get('first_name'): s+=10
    if lead.get('area'): s+=10
    if lead.get('price_range'): s+=10
    if lead.get('timeline'): s+=10
    notes=(lead.get('notes','')+lead.get('timeline','')).lower()
    if any(w in notes for w in ['asap','urgent','now','30 days']): s+=15
    return min(s,100)

def is_dup(lead):
    key=lead.get('phone','')+':'+lead.get('email','').lower()
    if not key or key==':': return False
    if key in seen_leads: return True
    seen_leads[key]=datetime.now().isoformat()
    return False

def detect_intent(lead):
    ex=lead.get('intent','').lower()
    if ex in ['buyer','seller','investor']: return ex
    notes=(lead.get('notes','')+lead.get('source','')).lower()
    if any(w in notes for w in ['sell','list','expired','fsbo']): return 'seller'
    if any(w in notes for w in ['invest','rental','cap rate']): return 'investor'
    return 'buyer'

def normalize(source,raw):
    lead={'source':source,'first_name':'','last_name':'','email':'','phone':'',
          'intent':'buyer','area':'Nashville','price_range':'','timeline':'',
          'notes':'','received_at':datetime.now().isoformat()}
    if source=='zillow':
        l=raw.get('lead',raw)
        nm=l.get('name',''); pts=nm.strip().split(' ',1) if nm else ['','']
        lead.update({'first_name':l.get('firstName',pts[0] if pts else ''),
                     'last_name':l.get('lastName',pts[1] if len(pts)>1 else ''),
                     'email':l.get('email',''),'phone':l.get('phone',''),
                     'area':l.get('market',l.get('city','Nashville')),
                     'notes':l.get('message',''),'intent':'buyer' if l.get('buyerLead',True) else 'seller'})
    elif source=='facebook':
        flds={}
        for e in raw.get('entry',[raw]):
            for c in e.get('changes',[{'value':e}]):
                for f in c.get('value',{}).get('field_data',[]):
                    flds[f.get('name','').lower()]=f.get('values',[''])[0]
        nm=flds.get('full_name',flds.get('name','')); pts=nm.strip().split(' ',1) if nm else ['','']
        lead.update({'first_name':flds.get('first_name',pts[0] if pts else ''),
                     'last_name':flds.get('last_name',pts[1] if len(pts)>1 else ''),
                     'email':flds.get('email',''),'phone':flds.get('phone_number',flds.get('phone','')),
                     'area':flds.get('city',flds.get('area','Nashville')),
                     'timeline':flds.get('timeline',''),'notes':flds.get('message','')})
    elif source=='redx':
        lt=raw.get('leadType',raw.get('type','expired'))
        lead.update({'first_name':raw.get('firstName',raw.get('first_name','')),
                     'last_name':raw.get('lastName',raw.get('last_name','')),
                     'email':raw.get('email',''),'phone':raw.get('phone',''),
                     'area':raw.get('city',raw.get('area','')),
                     'notes':'REDX '+lt.upper()+' | '+raw.get('address',''),
                     'timeline':'ASAP' if lt=='expired' else '','intent':'seller'})
    elif source=='manual':
        lead.update({'first_name':raw.get('first_name',''),'last_name':raw.get('last_name',''),
                     'email':raw.get('email',''),'phone':raw.get('phone',''),
                     'area':raw.get('area','Nashville'),'price_range':raw.get('price_range',''),
                     'timeline':raw.get('timeline',''),'notes':raw.get('notes',''),
                     'intent':raw.get('intent','buyer')})
    lead['phone']=clean_phone(lead.get('phone',''))
    lead['email']=lead.get('email','').lower().strip()
    lead['intent']=detect_intent(lead)
    return lead

def route(source,raw):
    lead=normalize(source,raw)
    lead['score']=score_lead(lead)
    if is_dup(lead): return {'status':'duplicate','name':lead.get('first_name','')+' '+lead.get('last_name','')}
    sms=email=False
    if lead.get('phone') and SMS_AGENT_URL:
        try:
            r=requests.post(SMS_AGENT_URL+'/outbound/send',
                json={'leads':[{'name':lead.get('first_name','')+' '+lead.get('last_name',''),
                                'phone':lead['phone'],'intent':lead.get('intent','buyer'),
                                'area':lead.get('area','Nashville'),'source':source}]},timeout=10)
            sms=r.status_code==200
        except: pass
    elif lead.get('phone'): logger.info('[DRY RUN SMS] '+lead.get('phone',''))
    if lead.get('email') and EMAIL_AGENT_URL:
        try:
            r=requests.post(EMAIL_AGENT_URL+'/leads/enroll',
                json={k:lead.get(k,'') for k in ['email','first_name','last_name','phone','intent','area','price_range','timeline','source']},
                timeout=10)
            email=r.status_code==200
        except: pass
    elif lead.get('email'): logger.info('[DRY RUN EMAIL] '+lead.get('email',''))
    result={'status':'routed','source':source,'name':lead.get('first_name','')+' '+lead.get('last_name',''),
            'phone':lead.get('phone'),'email':lead.get('email'),'intent':lead.get('intent'),
            'area':lead.get('area'),'score':lead['score'],'sms_sent':sms,'email_sent':email}
    logger.info('Routed: '+result['name']+'|'+source+'|score:'+str(result['score']))
    return result

@app.route('/webhook/zillow',methods=['POST'])
def zillow():
    data=request.get_json(silent=True) or {}
    leads=data.get('leads',[data])
    return jsonify({'processed':len(leads),'results':[route('zillow',l) for l in leads]})

@app.route('/webhook/facebook',methods=['GET','POST'])
def facebook():
    if request.method=='GET':
        if request.args.get('hub.verify_token')==FB_VERIFY_TOKEN:
            return request.args.get('hub.challenge',''),200
        return 'Forbidden',403
    data=request.get_json(silent=True) or {}
    results=[]
    for e in data.get('entry',[data]):
        for c in e.get('changes',[{'value':e}]):
            v=c.get('value',{})
            if v: results.append(route('facebook',v))
    return jsonify({'processed':len(results),'results':results})

@app.route('/webhook/redx',methods=['POST'])
def redx():
    data=request.get_json(silent=True) or {}
    leads=data.get('leads',[data])
    return jsonify({'processed':len(leads),'results':[route('redx',l) for l in leads]})

@app.route('/leads/add',methods=['POST'])
def add_lead():
    data=request.get_json(silent=True) or {}
    if not data: return jsonify({'error':'No data'}),400
    return jsonify(route('manual',data))

@app.route('/leads/bulk',methods=['POST'])
def bulk():
    data=request.get_json(silent=True) or {}
    leads=data.get('leads',[]); source=data.get('source','bulk')
    if not leads: return jsonify({'error':'No leads'}),400
    results=[route(source,l) for l in leads]
    return jsonify({'total':len(leads),'routed':sum(1 for r in results if r.get('status')=='routed'),
                   'duplicates':sum(1 for r in results if r.get('status')=='duplicate'),'results':results})

@app.route('/health')
def health():
    return jsonify({'status':'live','agent':'WWH Lead Router',
                   'sources':['zillow','facebook','redx','manual'],
                   'sms_agent':SMS_AGENT_URL or 'dry_run',
                   'email_agent':EMAIL_AGENT_URL or 'dry_run',
                   'leads_seen':len(seen_leads)})

if __name__=='__main__':
    app.run(host='0.0.0.0',port=int(os.environ.get('PORT',5003)),debug=False)