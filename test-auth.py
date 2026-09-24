import json, os, subprocess, tempfile, time, urllib.request, urllib.error, http.cookiejar
from pathlib import Path
ROOT=Path(__file__).resolve().parent
BASE='http://127.0.0.1:13123'
with tempfile.TemporaryDirectory(prefix='cps-auth-') as tmp:
    cfg={'proxyKey':'test-api-key-long-enough','publicBaseUrl':BASE,'accounts':[], 'knownModels':['test-model']}
    Path(tmp,'config.json').write_text(json.dumps(cfg))
    env={**os.environ,'DATA_DIR':tmp,'PORT':'13123','BIND_HOST':'127.0.0.1'}
    proc=subprocess.Popen([os.environ.get('NODE', 'node'),str(ROOT/'server.js')],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    def call(path, body=None, opener=None, origin=BASE, key=None):
        headers={'Origin':origin}
        if body is not None: headers['Content-Type']='application/json'
        if key: headers['Authorization']='Bearer '+key
        req=urllib.request.Request(BASE+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
        try:
            r=(opener.open(req,timeout=10) if opener else urllib.request.urlopen(req,timeout=10))
        except urllib.error.HTTPError as e: r=e
        return r.status,json.loads(r.read()),r.headers
    try:
        for i in range(100):
            try: call('/api/meta'); break
            except urllib.error.URLError: time.sleep(.05)
        else: raise RuntimeError(proc.stderr.read().decode())
        a=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        b=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        assert call('/api/accounts',key=cfg['proxyKey'])[0]==401
        for path in ['/v1/models','/api/v1/models','/models']:
            assert call(path)[0]==401
            assert call(path,key=cfg['proxyKey'])[0]==200
        assert call('/api/auth/login',{'password':cfg['proxyKey']},origin='https://evil.invalid')[0]==403
        assert call('/api/auth/login',{'password':'wrong'},a)[0]==401
        for client in [a,b]:
            status,_,headers=call('/api/auth/login',{'password':cfg['proxyKey']},client)
            assert status==200 and 'HttpOnly' in headers['Set-Cookie'] and 'SameSite=Strict' in headers['Set-Cookie']
            assert call('/api/accounts',opener=client)[0]==200
            assert call('/v1/models',opener=client)[0]==401
        assert call('/api/security',{},a,origin='https://evil.invalid')[0]==403
        assert call('/api/auth/password',{'currentPassword':'bad','newPassword':'test1234'},a)[0]==400
        assert call('/api/auth/password',{'currentPassword':cfg['proxyKey'],'newPassword':'1234567'},a)[0]==400
        assert call('/api/auth/password',{'currentPassword':cfg['proxyKey'],'newPassword':'test1234'},a)[0]==200
        for client in [a,b]: assert call('/api/accounts',opener=client)[0]==401
        assert call('/api/auth/login',{'password':cfg['proxyKey']},a)[0]==401
        assert call('/api/auth/login',{'password':'test1234'},a)[0]==200
        saved=json.loads(Path(tmp,'config.json').read_text())
        assert saved['proxyKey']==cfg['proxyKey'] and 'test1234' not in saved['adminPasswordHash']
        assert call('/v1/models',key=cfg['proxyKey'])[0]==200
        assert call('/api/auth/logout',{},a)[0]==200
        assert call('/api/accounts',opener=a)[0]==401
        proc.terminate(); proc.wait(timeout=10)
        proc=subprocess.Popen([os.environ.get('NODE', 'node'),str(ROOT/'server.js')],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        for i in range(100):
            try: call('/api/meta'); break
            except urllib.error.URLError: time.sleep(.05)
        assert call('/api/auth/login',{'password':'test1234'},a)[0]==200
        for i in range(10): assert call('/api/auth/login',{'password':'wrong'})[0]==401
        assert call('/api/auth/login',{'password':'wrong'})[0]==429
        print('PASS: migration, route isolation, login, CSRF, cookie, password validation/change, all-session revocation, API preservation, persistence, logout, rate limit')
    finally:
        proc.terminate(); proc.wait(timeout=10)
