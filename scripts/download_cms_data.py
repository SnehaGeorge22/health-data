# download_cms_data_fixed.py

import requests
from pathlib import Path
import zipfile

# VERIFIED URLs - Updated Jan 2026
files_to_download = {
    'beneficiary': [
        {
            'filename': 'DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip',
            'url': 'https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_beneficiary_summary_file_sample_1.zip'
        },
        {
            'filename': 'DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip',
            'url': 'https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2009_beneficiary_summary_file_sample_1.zip'
        },
        {
            'filename': 'DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip',
            'url': 'https://www.cms.gov/sites/default/files/2020-09/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip'
        }
    ],
    'inpatient': [
        {
            'filename': 'DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip',
            'url': 'https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_inpatient_claims_sample_1.zip'
        }
    ],
    'outpatient': [
        {
            'filename': 'DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip',
            'url': 'https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads/de1_0_2008_to_2010_outpatient_claims_sample_1.zip'
        }
    ],
    'carrier': [
        {
            'filename': 'DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip',
            'url': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip'
        },
        {
            'filename': 'DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip',
            'url': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip'
        }
    ],
    'prescription': [
        {
            'filename': 'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip',
            'url': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip'
        }
    ]
}

def download_file(url, destination):
    """Download a file with progress indication"""
    print(f"\nDownloading: {destination.name}")
    print(f"From: {url}")
    
    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        
        with open(destination, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
                print("✓ Downloaded (size unknown)")
            else:
                downloaded = 0
                for chunk in response.iter_content(chunk_size=block_size):
                    f.write(chunk)
                    downloaded += len(chunk)
                    percent = (downloaded / total_size) * 100
                    mb_downloaded = downloaded / (1024 * 1024)
                    mb_total = total_size / (1024 * 1024)
                    print(f"\rProgress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end='')
                print()
                print(f"✓ Downloaded: {destination.name} ({mb_total:.1f} MB)")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error downloading {destination.name}: {e}")
        return False

def extract_zip(zip_path, extract_to):
    """Extract ZIP file"""
    print(f"Extracting: {zip_path.name}")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            members = zip_ref.namelist()
            print(f"  Found {len(members)} file(s) in archive")
            zip_ref.extractall(extract_to)
            for member in members:
                print(f"  ✓ {member}")
        print(f"✓ Extraction complete")
        return True
    except Exception as e:
        print(f"✗ Extraction error: {e}")
        return False

def main():
    download_dir = Path('cms_data_downloads')
    download_dir.mkdir(exist_ok=True)
    
    total_files = sum(len(files) for files in files_to_download.values())
    current_file = 0
    
    print(f"{'='*70}")
    print(f"CMS DE-SynPUF Data Download - Sample 1")
    print(f"Total files to download: {total_files}")
    print(f"{'='*70}")
    
    for category, file_list in files_to_download.items():
        print(f"\n{'='*70}")
        print(f"CATEGORY: {category.upper()}")
        print(f"{'='*70}")
        
        category_dir = download_dir / category
        category_dir.mkdir(exist_ok=True)
        
        extract_dir = category_dir / 'extracted'
        extract_dir.mkdir(exist_ok=True)
        
        for file_info in file_list:
            current_file += 1
            filename = file_info['filename']
            url = file_info['url']
            destination = category_dir / filename
            
            print(f"\n[{current_file}/{total_files}] Processing: {filename}")
            
            # Download
            if not destination.exists():
                success = download_file(url, destination)
                if not success:
                    print(f"⚠️  Skipping {filename} due to download error")
                    continue
            else:
                file_size_mb = destination.stat().st_size / (1024 * 1024)
                print(f"⊙ Already exists: {filename} ({file_size_mb:.1f} MB)")
            
            # Extract
            if destination.suffix == '.zip' and destination.exists():
                extract_zip(destination, extract_dir)
    
    print(f"\n{'='*70}")
    print("✓ DOWNLOAD PROCESS COMPLETE!")
    print(f"{'='*70}")
    print(f"\nDownloaded files are in: {download_dir.absolute()}")
    print(f"CSV files are in the 'extracted' subfolders")
    print(f"\nNext step: Run upload_to_s3.py to upload to AWS")

if __name__ == "__main__":
    main()