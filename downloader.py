import threading
from pathlib import Path
from typing import List

try:
    import requests
    from requests import Response
    from requests.auth import HTTPBasicAuth
    from tqdm import tqdm
except ModuleNotFoundError: raise ModuleNotFoundError('''Please install the following packages...\n\nconda install -c anaconda requests\nconda install -c conda-forge tqdm\n''')


decorator = lambda fn, *_, **__: lambda *a, **kw: fn(*a, **kw)
try:
   from email_tools import Email, email_notification_wrapper
   error_notif = lambda yes: email_notification_wrapper if yes else decorator
   email_notif = lambda *args, **kwargs: Email().send(*args, **kwargs)
except ImportError:
   print(f'WARNING: Package email-tools not installed ! Use < pip install git+https://github.com/AndreGraca98/email-tools.git > to install package\n')
   error_notif = lambda yes: decorator
   email_notif = lambda *args, **kwargs: None
   
   

class Downloader:
    def __init__(self, url:str, user:str=None, pwd:str=None, zips_dir:str='.', notify=False):
        self.url=url
        self.auth = HTTPBasicAuth(user, pwd) if user and pwd else None
        self.zips_dir = Path(zips_dir)
        self.notify = notify
        
        self._make_dir(_dir=self.zips_dir)
        
        print('URL:', self.url)
        print('USER:', user,)
        print('PWD:', pwd)
        print(f'Destination directory: {str(self.zips_dir.resolve())}')
        
    def _make_dir(self, _dir: Path):
        """ Creates the destination folder if it doesnt exist already """
        if not _dir.is_dir():
            print(f'Creating directory: {_dir}')
            _dir.mkdir(parents=True)

    def __get_url_content(self):
        response = self.get_request(self.url)
        content = response.content

        return content
        
    def __download_target(self, url_target_path:str):
        """ Downloads a target path

        Args:
            url_target_path (str): target path 
        """

        response = self.get_request(self.url + url_target_path, stream=True)
        
        remote_filesize = int(response.headers.get('content-length', 0))
        
        target_file = self.zips_dir / url_target_path
        
        self._make_dir(target_file.parent)
        
        if target_file.is_file():        
            local_filesize = target_file.stat().st_size
        
            if local_filesize == remote_filesize: # if download finished
                print(f'File < {target_file} > already exists! ')
                return
            
        with tqdm(total=remote_filesize, unit='iB', unit_scale=True, desc=f'Downloading to: {str(target_file)}') as pbar:
            with open(str(target_file), 'wb') as file:
                for data in response.iter_content(1024): #1 Kibibyte
                    pbar.update(len(data))
                    file.write(data)
        
        if remote_filesize != 0 and pbar.n != remote_filesize: print("ERROR, something went wrong")

    def get_request(self, full_target_url:str, stream=False) -> Response:
        """ Processes a url request

        Args:
            full_target_url (str): URL target
            stream (bool, optional): reponse as a stream. Defaults to False.

        Raises:
            ConnectionError: if cant connect to the url

        Returns:
            Response: url response
        """
        response = requests.get(full_target_url, auth = self.auth, stream=stream)
        if response.status_code != 200: raise ConnectionError(f'URL:{full_target_url}\nGot response: {response}')
        
        return response
            
    def get_targets_list(self) -> List[str]:
        """ Retrieves all the targets 

        Returns:
            List[str]: List of targets to download
        """
        NOT_IMPLEMENTED_MSG = '''Please implement this method as follows:
        
    def get_targets_list(self) -> List[str]:
        content = super().get_targets_list()
        
        # Do something with the content of the url here
        TARGETS_LIST = ...
        
        return TARGETS_LIST
        '''
        if self.__class__.__name__.lower() == 'Downloader'.lower(): raise NotImplementedError(NOT_IMPLEMENTED_MSG)
        return self.__get_url_content()

    def download(self):
        """ Downloads all dataset parts in sequential order """
        targets = self.get_targets_list()
        
        for target in targets:
            self.__download_target(target)
            
        if self.notify: email_notif('Done downloading dataset')

    def download_with_threads(self, max_threads:int=4):  
        """ Downloads all dataset parts using threads

        Args:
            max_threads (int, optional): Maximum number of threads to use. Defaults to 4.
        """
        
        targets = self.get_targets_list()
        
        while len(targets)>0:
            items_length = max_threads if len(targets) > max_threads else len(targets) # Remaining targets
            
            thread_targets = [targets.pop(0) for _ in range(items_length)]
                
            threads=[]
            for target in thread_targets:
                t1 = threading.Thread(target=self.__download_target, args=[target])
                t1.start()
                threads.append(t1)
                
            for thread in threads:
                thread.join()
        
        if self.notify: email_notif('Done downloading dataset')


# ENDFILE
