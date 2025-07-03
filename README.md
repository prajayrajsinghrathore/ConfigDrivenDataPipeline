# ConfigDrivenDataPipeline
Create a unified, configuration-driven data ingestion platform that replaces ADF pipelines with a single, reusable architecture. The solution will dynamically handle various data sources through YAML configurations, perform customisable validation and enrichment. 


## Setup Instructions

### 1. Setup WSL (Windows Subsystem for Linux)

#### Prerequisites
- Windows 10 version 2004 and higher (Build 19041 and higher) **or** Windows 11.

#### Installation Steps
1. **Open PowerShell as Administrator**  
   Run the following command to install WSL:
   ```sh
   wsl --install
   ```
2. **Install Ubuntu 22.04**  
   Run the following command:
   ```sh
   wsl --install -d Ubuntu-22.04
   ```

---

### 2. Install Docker Desktop

- **Download Docker Desktop:**  
  [Docker Desktop: The #1 Containerization Tool for Developers | Docker](https://www.docker.com/products/docker-desktop/)  
- **Install Docker Desktop** on your machine.

#### Configure Docker Desktop
- Go to **Settings > General**
  - Check: `Use containerd for pulling and storing images`
  - Uncheck: `Send Usage Statistics`
- Go to **Settings > Resources**
  - Enable: `Resource Saver`
- Click on **File Sharing**
  - Add the folder path where your project will be stored (use the `+` button to confirm). This is basically the folder where you hev cloned this repository.
- Go to **WSL Integration**
  - Ensure `Enable Integration with my default WSL distro` is checked.

---

### 3. Install Python 3.12

- **Download Python 3.12:**  
  [Download Python | Python.org](https://www.python.org/downloads/)

---

### 4. Install Visual Studio Code

- **Download VS Code:**  
  [Visual Studio Code - Code Editing. Redefined](https://code.visualstudio.com/)

---

### 5. Install Apache Airflow Extension in VS Code

- Open VS Code.
- Go to **Extensions** and search for `Airflow`.
- Install the **Airflow extension**.

---
### 6. Download Git
- Download Git for windows https://gitforwindows.org/
- During installation, ensure you check to put git on environment path 

### 7. Clone Repository

Clone using 
```sh
git clone https://github.com/prajayrajsinghrathore/ConfigDrivenDataPipeline.git
```
or 

```sh
# using ssh
git clone git@github.com:prajayrajsinghrathore/ConfigDrivenDataPipeline.git
```

Switch to the develop branch
```sh
 git checkout develop
```

### 8. Installing Python Requiements
Before we start development we will install python requirements
- Go to the Repository
- Start PowerShell Terminal here and run command

```sh
pip install -r /requirements.txt
```
- This will take some time as there are large dependencies that needs reolving. 

### 9. Check
- Docker Desktop is running
- You have followed all the above steps or satisfy requirements in the above steps. 
- Open .env file and edit the AIRFLOW_PROJ_DIR folder to

```sh
AIRFLOW_PROJ_DIR=Path to your repository

# For example
# AIRFLOW_PROJ_DIR=F:\Sandbox\ConfigDrivenDataPipeline

# Ensure there are no spaces in the folder names or file names in the path.
```
- Open PowerShell and run command

```sh
docker-build -t airflow:latest .

# This will build the image and install the dependencies.
```
 - Once the build is finished, run command
 
 ```sh
 # For the first time use just docker-compose up
 # Once it is all green you can then next time do docker-compose up -d to avoid engaging terminal with logs. 
 docker-compose up 
 ```

 - Wait for it to go all green and running
 - You can open Docker Desktop and Click Containers. Noticee containers are now created and they will start to appear green.
 - The Airflow-init-1 container will apeear as not running. It is normal.

- Go to Web browser and open 

```ps
http://localhost:8080/
```
- Login using username: airflow and password: airflow
- Congratulations you have now setup everything.  

- To bring down the containers
```sh
docker-compose down
```

- To get conainer up and running again
```sh
docker-compose up -d
```