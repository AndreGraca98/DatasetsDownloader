import argparse
import os
from pathlib import Path
from glob import glob
from typing import List

try: import bs4
except ModuleNotFoundError: raise ModuleNotFoundError('''Please install the following packages...\n\nconda install -c anaconda beautifulsoup4\npip install lxml\n''')

   
from downloader import Downloader, error_notif, email_notif
   
   
   
class SiWBuilder(Downloader):
    def __init__(self, url: str, user: str = None, pwd: str = None, dst_dir: str = '.', notify=False):
        super().__init__(url, user, pwd, dst_dir, notify)
    
    def get_targets_list(self) -> List[str]:
        content = super().get_targets_list()
        
        soup = bs4.BeautifulSoup(content, features="lxml")
        parts = list(map(lambda x: x.find('a').string.strip(' ') , soup.find_all('li')))

        urls2download = [ part for part in parts if 'tar.gz' in part ]

        return sorted(urls2download)

    def unzip(self, to_folder:str='../'):
        """ Combines all the ziped parts and unzips the complete zip to a folder

        Args:
            to_folder (str, optional): Unziped destination folder. Defaults to '../'.
        """
        self.assert_correct_total_size()
        
        target_dir = str(self.dst_dir.resolve())
        to_folder = Path(to_folder).resolve()
        
        
        # COMBINE ZIP PARTS
        complete_zip_path = self.dst_dir.resolve() / 'SiW.tar.gz'
        parts_joined = complete_zip_path.is_file() and complete_zip_path.stat().st_size == self.total_remote_size
        if parts_joined: print(f"Complete zip file already combined in {complete_zip_path} !")
        
        combine_parts_cmd = f'cd {target_dir} && cat SiW.tar.gz.part* > SiW.tar.gz && cd ..'
        if not parts_joined: print(f'$ {combine_parts_cmd}'); os.system(combine_parts_cmd)
        
        
        # UNZIP COMPLETE ZIP
        total_unziped_size = 0
        for path, dirs, files in os.walk(f'{to_folder / "SiW_release/"}'):
            for f in files: total_unziped_size += os.path.getsize(os.path.join(path, f))

        unziped = (to_folder / 'SiW_release').is_dir() and total_unziped_size == 226629440558 # already pre calculated ≃ 226 GB
        if unziped: print(f"Complete unziped dataset already extracted to { to_folder / 'SiW_release/' } !")
        
        unzip_cmd = f'cd {target_dir} && tar -xf SiW.tar.gz -C {to_folder} && cd ..'
        if not unziped: print(f'$ {unzip_cmd}'); os.system(unzip_cmd)
        
        
        if self.notify: email_notif('Done unziping SiW')

    def assert_correct_total_size(self):
        """ Makes sure all the files were correctly downloaded

        Raises:
            ConnectionError: If cant access the website

        """
        B2GB = lambda x: round(x/(1024**3), 2) # B/(1024**3) = KB/(1024**2) = MB/(1024**1) = GB
        
        # Calculate local file size
        def calculate_local_file_size(verbose=False):
            total_size = 0
            local_files = glob(str(self.dst_dir.resolve() / 'SiW.tar.gz.part-*'))            
            for local_file in local_files:
                total_size += Path(local_file).stat().st_size
                if verbose: print('Local:', B2GB(total_size), 'GB', end='\r')
        
            return total_size
        
        # Calculate remote file size
        def calculate_remote_file_size(verbose=False):
            total_size = 0
            targets = self.get_targets_list()
            for target in targets:
                response = self.get_request(self.url + target, stream=True)
                if response.status_code != 200: raise ConnectionError(f'Got response: {response}')
                total_size += int(response.headers.get('content-length', 0))
                if verbose: print('Remote:', B2GB(total_size), 'GB', end='\r')

            return total_size


        self.total_local_size = calculate_local_file_size(verbose=False)
        self.total_remote_size = 225077831964 # Already calculated to avoid redundant requests ≃ 225 GB
        # self.total_remote_size = calculate_remote_file_size(verbose=False)
          
        assert self.total_remote_size == self.total_local_size, f'Total downloaded file size does not match total remote file size! Local: {B2GB(self.total_local_size)} GB ; Remote: {B2GB(self.total_remote_size)} GB'




def main(args, _help):
    
    @error_notif(yes=args.notification)
    def __main():
        siw_builder = SiWBuilder(url='https://www.egr.msu.edu/computervision/SiW_database/', user=args.user, pwd=args.password, dst_dir='zips')
        
        if args.download: print('DOWNLOADING...'); siw_builder.download_with_threads(max_threads=4) # siw_builder.download()
        if args.unzip: print('UNZIPING...'); siw_builder.unzip(to_folder='../')
        
        if not (args.download or args.unzip): print(_help)
        
    __main()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', type=str, dest='user', required=True, help='User to access the SiW database')
    parser.add_argument('--pwd', '--password', type=str, dest='password', required=True, help='Password to access the SiW database')
    
    parser.add_argument('-d', '--download', action='store_true', dest='download', help='Download the SiW dataset')
    parser.add_argument('-u', '--unzip', action='store_true', dest='unzip', help='Unzip the SiW dataset')
    parser.add_argument('-n', '--notif', '--notification', action='store_true', dest='notification', help='Send notification on error, or on download or unzip end')
    
    args = parser.parse_args()
    
    main(args, _help=parser.format_help())



# ENDFILE