import sys
import os, shutil
import signal
import subprocess
import platform
import time
import json

def run_cmd(cmd_string, timeout=600):
    #print("命令为：" + cmd_string)
    p = subprocess.Popen(cmd_string, stderr=subprocess.DEVNULL, stdout=subprocess.PIPE, shell=True, close_fds=True,
                         start_new_session=True)
    format = 'gbk' if platform.system() == "Windows" else 'utf-8'
    #time.sleep(2)

    #p.kill()

    #stdout = p.stdout.read()
    #stderr = p.stderr.read()

    #input(">>> ")
    #print(stdout)
    #input(">>> ")
    #print(stderr)

    # exit(1)

    try:
        (msg, errs) = p.communicate(timeout=timeout)
        if ret_code := p.poll():
            code = 1
            msg = f"[Error]Called Error ： {str(msg.decode(format))}"
        else:
            code = 0
            msg = str(msg.decode(format))
    except subprocess.TimeoutExpired:
        # 注意：不能只使用p.kill和p.terminate，无法杀干净所有的子进程，需要使用os.killpg
        p.kill()
        p.terminate()
        os.killpg(p.pid, signal.SIGTERM)

        # 注意：如果开启下面这两行的话，会等到执行完成才报超时错误，但是可以输出执行结果
        # (outs, errs) = p.communicate()
        # print(outs.decode('utf-8'))

        code = 1
        msg = f"[ERROR]Timeout Error : Command '{cmd_string}' timed out after {str(timeout)} seconds"
    except Exception as e:
        code = 1
        msg = f"[ERROR]Unknown Error : {str(e)}"

    return code, msg