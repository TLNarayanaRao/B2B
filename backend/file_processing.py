"""Optional File connector post-processing with token-expanded argument vectors."""
import json
import os
import subprocess
from .protocols import ConnectorError, secret


def post_process(root,location,options):
    try:
        command=json.loads(secret(options.post_process_command_env,True))
    except (ValueError,TypeError) as exc:
        raise ConnectorError("Post-process value must be a JSON argument array; the file is already stored") from exc
    if not isinstance(command,list) or not command or not all(isinstance(arg,str) and "\0" not in arg for arg in command):
        raise ConnectorError("Post-process command must be a nonempty array of strings; the file is already stored")
    values={"file":str(location),"filename":location.name,"path":str(location.parent)+os.sep,
        "base":location.stem,"noext":str(location.with_suffix("")),"ext":location.suffix}
    argv=[]
    for item in command:
        for key,value in values.items():
            item=item.replace(chr(36)+"{"+key+"}",value)
        if chr(36)+"{" in item:
            raise ConnectorError("Post-process supports file, filename, path, base, noext and ext tokens; the file is already stored")
        argv.append(item)
    working=(root/options.post_process_working_directory).resolve(strict=True)
    if not working.is_relative_to(root) or not working.is_dir():
        raise ConnectorError("Post-process working directory must remain within the File root; the file is already stored")
    try:
        result=subprocess.run(argv,cwd=working,shell=False,timeout=options.post_process_timeout,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name=="nt" else 0)
    except (OSError,subprocess.TimeoutExpired) as exc:
        raise ConnectorError("File stored but post-processing failed or timed out; outcome uncertain, review before retrying") from exc
    if result.returncode!=0:
        raise ConnectorError("File stored but post-processing returned failure; outcome uncertain, review before retrying")
