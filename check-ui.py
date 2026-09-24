import json, os, subprocess, tempfile, time, urllib.request
from pathlib import Path
from playwright.sync_api import sync_playwright
R=Path(__file__).resolve().parent
live=os.environ.get('LIVE')=='1'
base=os.environ['BASE_URL'] if live else 'http://127.0.0.1:13124'
tmp=tempfile.TemporaryDirectory(prefix='cps-ui-')
proc=None
password=os.environ['TEST_ADMIN_PASSWORD'] if live else 'isolated-password-123'
if not live:
    Path(tmp.name,'config.json').write_text(json.dumps({'proxyKey':password,'publicBaseUrl':base,'accounts':[],'knownModels':['test-model']}))
    Path(tmp.name,'metadata.json').write_text(json.dumps({'models':{},'history':[],'catalog':['test-model','vendor/example-model'],'catalogFetchedAt':int(time.time()*1000)}))
    proc=subprocess.Popen([os.environ.get('NODE', 'node'),str(R/'server.js')],env={**os.environ,'PORT':'13124','DATA_DIR':tmp.name},stdout=subprocess.DEVNULL)
    for _ in range(100):
        try: urllib.request.urlopen(base+'/api/meta'); break
        except Exception: time.sleep(.05)
try:
 with sync_playwright() as p:
    browser=p.chromium.launch(headless=True,args=['--no-sandbox'])
    errors=[]
    for width in [1440,390,320]:
        ctx=browser.new_context(viewport={'width':width,'height':900},is_mobile=width<500,has_touch=width<500)
        page=ctx.new_page(); page.on('pageerror',lambda e: errors.append(str(e)))
        page.goto(base,wait_until='networkidle')
        page.locator('#loginKey').fill(password); page.locator('#loginBtn').click()
        page.locator('#appShell').wait_for(state='visible')
        page.wait_for_function("DATA !== null")
        for view in ['models','accounts','test','history','catalog','security']:
            if width<500: page.locator('#menuToggle').click()
            page.locator('nav [data-view="'+view+'"]').click()
            page.locator('#view-'+view).wait_for(state='visible')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'),(width,view,'overflow')
        if not live:
            page.evaluate("location.hash='models'")
            page.once('dialog',lambda dialog: dialog.accept())
            page.locator('.remove-model[data-model="test-model"]').click()
            page.wait_for_function("!DATA.subscription.some(s=>s.id==='test-model')")
            assert 'test-model' in page.evaluate('DATA.catalog')
            req=urllib.request.Request(base+'/v1/models',headers={'Authorization':'Bearer isolated-password-123'})
            assert 'test-model' not in [m['id'] for m in json.load(urllib.request.urlopen(req))['data']]
            assert 'test-model' in json.loads(Path(tmp.name,'config.json').read_text())['removedModels']
            page.reload(wait_until='networkidle'); page.wait_for_function('DATA !== null')
            assert not page.evaluate("DATA.subscription.some(s=>s.id==='test-model')")
            page.evaluate("location.hash='catalog'")
            restore=page.locator('.catalog-select[data-model="test-model"]')
            assert restore.inner_text()=='加入模型列表'
            restore.click(); page.wait_for_function("DATA.subscription.some(s=>s.id==='test-model')")
            assert 'test-model' in [m['id'] for m in json.load(urllib.request.urlopen(req))['data']]
            print('PASS subscription removal, retained catalog, persistence, restore',width)
            page.evaluate("location.hash='models'")
            # Stub only official discovery: no dependency on public services for timer tests.
            page.route('**/api/fetch-official-models',lambda route: route.fulfill(json={'ok':True,'added':[],'sources':['fixture'],'found':1,'knownModels':['test-model']}))
            fetch=page.get_by_role('button',name='⬇ 拉取官方最新模型')
            fetch.click(); page.get_by_role('button',name='关闭拉取结果').wait_for(state='visible')
            page.locator('#fetchResult').wait_for(state='hidden',timeout=9000)
            fetch.click(); page.get_by_role('button',name='关闭拉取结果').click()
            assert not page.locator('#fetchResult').is_visible()
            page.unroute('**/api/fetch-official-models')
            page.route('**/api/fetch-official-models',lambda route: route.fulfill(status=500,json={'error':{'message':'fixture failure'}}))
            fetch.click(); page.get_by_role('button',name='关闭拉取结果').wait_for(state='visible')
            page.locator('#fetchResult').wait_for(state='hidden',timeout=9000)
            page.unroute('**/api/fetch-official-models')
            print('PASS fetch result auto-dismiss, manual close, failure auto-dismiss',width)
            page.evaluate("location.hash='catalog'")
            button=page.locator('.catalog-select[data-model="vendor/example-model"]')
            button.wait_for(state='visible'); button.click()
            page.wait_for_function("DATA.selectedCatalogModels.includes('vendor/example-model')")
            assert button.inner_text()=='移除'
            assert page.locator('#testModel option[value="vendor/example-model"]').count()==1
            req=urllib.request.Request(base+'/v1/models',headers={'Authorization':'Bearer isolated-password-123'})
            assert 'vendor/example-model' in [m['id'] for m in json.load(urllib.request.urlopen(req))['data']]
            assert 'vendor/example-model' in json.loads(Path(tmp.name,'config.json').read_text())['selectedCatalogModels']
            page.reload(wait_until='networkidle'); page.wait_for_function('DATA !== null')
            assert button.inner_text()=='移除'
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            button.click(); page.wait_for_function("!DATA.selectedCatalogModels.includes('vendor/example-model')")
            assert 'vendor/example-model' not in [m['id'] for m in json.load(urllib.request.urlopen(req))['data']]
            assert page.locator('#testModel option[value="vendor/example-model"]').count()==0
            page.evaluate("location.hash='security'")
            page.locator('#view-security').wait_for(state='visible')
            print('PASS catalog add/remove, persisted selection, client list, test selector',width)
        if width==1440 and not live:
            page.locator('#currentPassword').fill(password)
            page.locator('#newPassword').fill('changed-password-456')
            page.locator('#confirmPassword').fill('changed-password-456')
            page.locator('#passwordBtn').click(); page.locator('#loginOverlay').wait_for(state='visible')
            page.locator('#loginKey').fill(password); page.locator('#loginBtn').click()
            page.locator('#loginErr').wait_for(state='visible')
            password='changed-password-456'
            page.locator('#loginKey').fill(password); page.locator('#loginBtn').click()
            page.locator('#appShell').wait_for(state='visible'); page.wait_for_function('DATA !== null')
        if os.environ.get('SCREENSHOT_DIR'):
            output=Path(os.environ['SCREENSHOT_DIR']); output.mkdir(parents=True,exist_ok=True)
            page.screenshot(path=str(output/f'ui-{width}.png'),full_page=True)
        if width<500: page.locator('#menuToggle').click()
        page.locator('#logoutBtn').click(); page.locator('#loginOverlay').wait_for(state='visible')
        assert not page.locator('#secProxyKey').input_value()
        ctx.close()
        print('PASS real backend UI',width,'all six views, login/logout, no overflow')
    assert not errors,errors
    browser.close()
finally:
 if proc: proc.terminate(); proc.wait(timeout=10)
 tmp.cleanup()
