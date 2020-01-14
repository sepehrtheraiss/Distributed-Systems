import docker
import requests
import time

def printres(r):
    print (r.status_code)
    print (r.headers)
    print (r.content)
    print

def dcrun(p):
    return client.containers.run(image, detach=True, ports={'8080/tcp' : p}, environment={"VIEW":','.join(view), "IP_PORT":IP+p})

uri   = "http://localhost:"
port  = ["8082","8083","8084"]
IP    = ["192.168.1.2","192.168.1.3","192.168.1.4"]
defP  = "8080"
hosts = dict(zip(IP,port))
endp  = { "kv" :"/keyValue-store/",
          "kvs":"/keyValue-store/search/",
          "v"  :"/view"}

key    = ["a","b","c"] 
#IP    = "192.168.1.12"
myIP   = "localhost"
#view   = [IP+p for p in port]
image  = "kv-store"
node   = []
client = docker.from_env()

def init():
    node = [dcrun(p) for p in port]
    print(node[0].logs())
    return node

def Get(req):
    print("request:",req)
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Get Request failed",req)

def testA(): 
    print ("***********************************")
    print ("\nCase A: Single host, single key\n")
    req = uri+port[0]+endp["kv"]+key[0]
    print (req)
    print

    print ("\nCase 1A: Get key does not exist, empty causual payload\n")
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 2A: Put key, empty causual payload\nGet key should exist\n")
    try:
        r = requests.put(req, val = "1")
        printres(r)
    except :
        print("Request failed")

    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

def testB():
    print ("***********************************")
    print ("\nCase B: Multiple hosts, single key\n")

    for p in port:
        req = uri+p+endp["kv"]+key[0]
        print(req)

        print ("\nCase 1B: Get key should exist\n")
        try:
            r = requests.get(req)
            printres(r)
        except:
            print("Request failed")

def testC():
    print ("***********************************")
    print ("\nCase C: Kill {}, single key\n".format(node[0]))

    node[0].kill()
    req = uri+port[0]+endp["kv"]+key[0]
    print(req)

    print ("\nCase 1C: Get key should fail\n")
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 2C: Revise dead node, Get request should exist\n")

    node[0] = dcrun(port[0])
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

def testD():
    print ("***********************************")
    print ("\nCase D: View\n")

    req = uri+port[0]+endp["v"]
    print(req)
    print ("\nCase 1D: Get view single host\n")
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 2D: Put view single host\nGet new view\n")

    port.append("8085")
    IPP = "192.168.1.5" + ":" + port[-1]
    try:
        r = requests.put(req, data = {'ip_port' : IPP}) 
        printres(r)
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")
    
    print ("\nCase 3D: Put same view, single host\n")
    try:
        r = requests.put(req, data = {'ip_port' : IPP})
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 4D: Get view, another host\n")
    req = uri+port[1]+endp["v"]
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 5D: Delete a view, new view added, single host\n")
    req = uri+port[0]+endp["v"]
    try:
        r = requests.delete(req, data = {'ip_port' : IPP})
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 6D: Delete a view, deleted view, single host\n")
    try:
        r = requests.delete(req, data = {'ip_port' : IPP})
        printres(r)
    except:
        print("Request failed")

    print ("\nCase 7D: Get view, single host\n")
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

    
    req = uri+port[-1]+endp["v"]
    print ("\nCase 8D: Get view from 8083, single host\n")
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")

    req = uri+port[1]+endp["v"]
    print ("\nCase 9D: Get deleted view from 8082, single host\n")
    try:
        r = requests.get(req)
        printres(r)
    except:
        print("Request failed")


def testDp():
    print ("***********************************")
    print ("\nCase Dp: views\n")

    print ("\nCase 1D: Request view from node A and B\n")
    Get(uri+port[0]+endp["v"])
    Get(uri+port[1]+endp["v"])
    print ("\nCase 2D: Request view from 3'rd node\n")
    print ("\nNode A,B will start with the same view. Node C will only be aware of it self")
    print ("\nNode A,B will have the same view then should update C on gossip")
    Get(uri+port[2]+endp["v"])


def testF(node):
    print ("***********************************")
    print ("\nCase F: Parition\n")
    
    print ("\nCase 1F: 3 nodes up, 8081 goes down. 8080 sends msg to 8082, 8081 comes up recieves a get request\n") 
    #node[1].kill()
    req = uri+port[0]+endp["kv"]+key[0]
    try:
        print("PUT",req)
        r = requests.put(req, data = {'val' : 1})
        printres(r)
    except:
        print("Request failed")

#    node[1] = dcrun(port[1])
 #   time.sleep(2)

    req = uri+port[1]+endp["kv"]+key[0]
    try:
        print("GET",req)
        r = requests.get(req) 
        printres(r)
    except:
        print("Request failed")

def deinit(node):
    # Kill all containers
    for n in node:
        try :
            n.kill()
        except:
            print("node {} is already dead.".format(n))

#node = init()
#testDp()
#testF(node)
#deinit(node)

ipPort = "localhost:8082"
newAddress = "192.168.0.3"
#r = requests.put( 'http://%s/keyValue-store/%s'%(str(ipPort), "a"), data={'val':1, 'payload': 2})
#printres(r)
r = requests.put( 'http://%s/view'%str(ipPort), data={'ip_port':newAddress} )
printres(r)
r = requests.delete( 'http://%s/view'%str(ipPort), data={'ip_port':newAddress} )
printres(r)

