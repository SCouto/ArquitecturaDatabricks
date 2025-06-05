
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/SCouto/ArquitecturaDatabricks)

# Arquitectura Databricks

This repository contains Databricks notebooks for practicing data analysis and processing using both Python and Scala. The project provides both unsolved and solved versions of the notebooks, so you can practise without the solution and then check it if you are blocked. 

For the final project, a Faker-based script generator is included to create synthetic data if needed.

Not all notebooks are provider in both languages, for instance the AI related are only in python
## Repository Structure

```
/
├── Section_folder/
│   ├── python/
│   │   ├── notebook1.py            # Unsolved version
│   │   ├── notebook1_solved.py     # Solved version
│   ├── scala/
│   │   ├── notebook2.scala         # Unsolved version
│   │   ├── notebook2_solved.scala  # Solved version
├── datasets/
│   ├── dataset_name                # Dataset used in the notebooks
│   │   ├──dataset_file.csv         # File for the dataset (some dataset may have sevaral files)
│   │   ├──dataset_file.json        # File for the dataset (some dataset may have sevaral files)
├── README.md
```

## Getting Started

### Running the Notebooks
1. Open the notebooks folder and choose between Python (`.py`) or Scala (`.scala`).
2. Import notebook in Databricks UI by clicking on the `Import` button.
3. If you want to challenge yourself, start with the unsolved versions before checking the solutions.

### Final Project 
Dataset for the final project in under the `dataset/project` folder
If you want to create aditional syntetic data you can modify as you please and script in `7_proyecto/dataset_generator`

In order to do so
#### Steps to Run the Faker Generator:
0. Make sure you have python installed in your local environment
1. Navigate to the `7_proyecto/dataset_generator` folder.
2. Create a virtual environment:
   ```bash
   python -m venv venv
   ```
3. Activate the virtual environment:
    - On Windows:
      ```bash
      venv\Scripts\activate
      ```
    - On macOS/Linux:
      ```bash
      source venv/bin/activate
      ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Run the script:
   ```bash
   python generate_data.py
   ```
   With the default configuration this will create a `product_catalog.csv` with 50 rows and 5 `sales_data<INDEX>.csv` files with 25000 rows each. 
   You can modify it as you please

## Notes
- Ensure you have Python installed to run the Faker script.
- Databricks users should upload the dataset to DBFS before running Scala notebooks.
- The provided solutions should only be referenced after attempting the exercises.

Enjoy coding!

