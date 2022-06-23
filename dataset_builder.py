import argparse
from enum import Enum
import hashlib
import os
from pathlib import Path
from glob import glob
import threading
from typing import List
import pandas as pd

from tqdm import tqdm


try: import bs4
except ModuleNotFoundError: raise ModuleNotFoundError('''Please install the following packages...\n\nconda install -c anaconda beautifulsoup4\npip install lxml\n''')

   
from downloader import Downloader, error_notif, email_notif
   
   
   
class SiWBuilder(Downloader):
    def get_targets_list(self) -> List[str]:
        content = super().get_targets_list()
        
        soup = bs4.BeautifulSoup(content, features="lxml")
        parts = list(map(lambda x: x.find('a').string.strip(' ') , soup.find_all('li')))

        urls2download = [ part for part in parts if 'tar.gz' in part ]

        return sorted(urls2download)

    def unzip(self, to_folder:str='.'):
        """ Combines all the ziped parts and unzips the complete zip to a folder

        Args:
            to_folder (str, optional): Unziped destination folder. Defaults to '../'.
        """
        self.assert_correct_total_size()
        
        target_dir = str(self.zips_dir.resolve())
        to_folder = Path(to_folder).resolve()
        
        self._make_dir(to_folder)
        
        # COMBINE ZIP PARTS
        complete_zip_path = self.zips_dir.resolve() / 'SiW.tar.gz'
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
            local_files = glob(str(self.zips_dir.resolve() / 'SiW.tar.gz.part-*'))            
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

    def checksum(self, max_threads=1, verbose=False):
        tqdm.write('Checksum...')
        
        checksum_file = pd.read_csv(str(self.zips_dir / 'checksum.md'), names=['hash', '','file'], sep=' ')
        checksum_file.dropna(axis=1, inplace=True)
        files, hashs = checksum_file.file.to_numpy(), checksum_file.hash.to_numpy()

        __files = list(files)
        __hashs = list(hashs)
        
        def __checksum(_file, _hash, __results, __idx):
            filename = str(self.zips_dir / _file)

            md5_hash = hashlib.md5()
            with open(filename,"rb") as f:
                for byte_block in iter(lambda: f.read(4096),b""):
                    md5_hash.update(byte_block)
                
                result = md5_hash.hexdigest() == _hash
                
                __results[__idx]=result
                
            if verbose: tqdm.write(f'File: {_file}\t|\tchecksum matches: {result}\t|\tidx: {__idx}')
                    

        idx=0
        results=[False]*files.shape[0]
        bar = tqdm(range(len(results)))
        while len(__files)>0:
            
            items_length = max_threads if len(__files) > max_threads else len(__files) # Remaining targets

            thread_files = [__files.pop(0) for _ in range(items_length)]
            thread_hashs = [__hashs.pop(0) for _ in range(items_length)]
            
            threads=[]
            for tf,th in zip(thread_files, thread_hashs):
                thr = threading.Thread(target=__checksum, args=(tf, th, results, idx))
                thr.start()
                threads.append(thr)
                idx+=1

            for thread in threads: thread.join()
            
            bar.update(items_length)


        assert all(results), f'Checksum failed {results.count(False)} times, in items at index {[i for i, res in enumerate(results) if not res ]}'

    def download(self):
        super().download()

        self.checksum(max_threads=10, verbose=True)
        
    def download_with_threads(self, max_threads: int = 4):
        super().download_with_threads(max_threads)

        self.checksum(max_threads=10, verbose=True)
        
   
   
class S3DFMBuilder(Downloader):
    def get_targets_list(self) -> List[str]:
        content = super().get_targets_list()
        
        soup = bs4.BeautifulSoup(content, features="lxml")
        parts = list(filter(None, map(lambda x: x.get('href') , soup.find_all('a'))))

        urls2download = [ part for part in parts if 'http' not in part ]

        return sorted(urls2download, key=lambda x: x.lower())

    def unzip(self, to_folder:str='../'):
        """ Combines all the ziped parts and unzips the complete zip to a folder

        Args:
            to_folder (str, optional): Unziped destination folder. Defaults to '../'.
        """
        
        zips_dir = self.zips_dir
        to_folder = Path(to_folder)              

        cwd = set(Path.cwd().parts)
        for zip_file in zips_dir.resolve().glob('**/*.zip'):
            data = set(zip_file.parts)-set([str(zips_dir)])
            extra_folders = (data - cwd)
            extra_path = '/'.join(zip_file.parts[-len(extra_folders):])[:~3]

            self._make_dir(to_folder / extra_path)
                
            os.system(f'unzip -n -qq {zip_file} -d {to_folder / extra_path}')

    def assert_correct_total_size(self):
        raise NotImplementedError
    
    
    
class BUILDER(Enum):
    siw = SiWBuilder
    s3dfm = S3DFMBuilder

class BUILDER_URL(Enum):
    siw = 'https://www.egr.msu.edu/computervision/SiW_database/'
    s3dfm = 'https://groups.inf.ed.ac.uk/trimbot2020/DYNAMICFACES/'


assertion_builder_msg = f'Missing keys in BUILDER or BUILDER_URL enums. Got BUILDER keys:{set(BUILDER.__members__.keys())} ; BUILDER_URL keys:{set(BUILDER_URL.__members__.keys())}'
assert set(BUILDER.__members__.keys()) == set(BUILDER_URL.__members__.keys()), assertion_builder_msg


def main(args, _help):
    assertion_downloads_msg = f'Can only choose to download in sequential order or using threads. Got args.threads_download: {args.threads_download} ; args.download: {args.download}'
    assert not(args.threads_download and args.download), assertion_downloads_msg
    
    @error_notif(yes=args.notification)
    def __main():       
        ziped_dir = args.ziped_dir or args.builder+'_ziped'
        unziped_dir = args.unziped_dir or args.builder+'_unziped'
        builder = BUILDER[args.builder].value(url=BUILDER_URL[args.builder].value, user=args.user, pwd=args.password, zips_dir=ziped_dir)
        
        if args.threads_download: print('DOWNLOADING USING THREADS...'); builder.download_with_threads(max_threads=4)
        if args.download: print('DOWNLOADING...'); builder.download()
        if args.unzip: print('UNZIPING...'); builder.unzip(to_folder=unziped_dir)
        
        if not (args.threads_download or args.download or args.unzip): print(_help)
        
    __main()


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(type=str, dest='builder', choices=BUILDER._member_names_ , help='Builder')
    
    parser.add_argument('--user', type=str, dest='user', default=None, help='User')
    parser.add_argument('--pwd', '--password', type=str, dest='password', default=None, help='Password')
    parser.add_argument('--zdir','--ziped_dir', type=str, dest='ziped_dir', default=None, help='Ziped directory')
    parser.add_argument('--uzdir','--unziped_dir', type=str, dest='unziped_dir', default=None, help='UnZiped directory')
    
    parser.add_argument('-t', '--threads_download', action='store_true', dest='threads_download', help='Download the dataset using threads')
    parser.add_argument('-d', '--download', action='store_true', dest='download', help='Download the dataset in sequential order')
    parser.add_argument('-u', '--unzip', action='store_true', dest='unzip', help='Unzip the dataset')
    parser.add_argument('-n', '--notif', '--notification', action='store_true', dest='notification', help='Send notification on error, or on download and unzip end')
    
    parser_help = '\n'+parser.format_help()
    
    return parser, parser_help

if __name__ == '__main__':
    parser, parser_help = get_parser()
    args = parser.parse_args()
    
    main(args, _help=parser_help)



# ENDFILE