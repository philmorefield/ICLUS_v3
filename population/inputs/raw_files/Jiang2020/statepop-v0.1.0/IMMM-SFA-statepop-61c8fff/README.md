# Brief Introduction
This document explains the scripts used for the multi-state population projection
model. The model is composed of a number of scripts that together provide population projection according to different scenarios at the U.S. state level.

These scripts are:
- R scripts
  - Dom_Mig_Bi_Standardizer.R
  - Scenario_Construction.R
  - Multi_State_Model_Bilateral.R
  - First_Pop_Add.R
  - State_Level_Migration.R
  - Pop_Dis_Vis.R
  - Regional_Migration_Vis.R
  - National_Pop_Vis_IIASA.R
  - Fert_Int_Mig.R
  - Single_State_Vis.R
- Python scripts
  - state_level_pop_vis.py
  - state_level_upscaled_vis.py

This set includes scripts for data preparation, running the model itself, and generating different additional results from the model and its initial outputs.

# More Details on Scripts
In order to produce results, the scripts should be run in a particular order. In the following, we demonstrate the order and what each scripts does.

- Dom_Mig_Bi_Standardizer.R (Prepares input)
  - Standardizes bilateral migration rates. It takes the original master table containing bilateral
	migration rates and creates two csv files per state. One is in-migration that includes migration rates to each state (including DC) from all other states at 1-year intervals for 101 age groups. The other is out-migration that includes migration rates from each state to all other states following the same specifications.

- Scenario_Construction.R (Prepares input)
  - Creates scenarios (SSP2, SSP3 and SSP5) based on assumptions on TFR, life expectancy at age 0,
	international migration and domestic migration by incorporating historical records and IIASA's equivalent projections. This script should be run if SSP-based scenarios are required for projection. For the other scenarios, this script is not needed, and they can be accounted for by modifying the initial parameters of the main script.

- Multi_State_Model_Bilateral.R (Main model)
  - This is the main script that projects population by states for the U.S. The model projects different state-level population estimates based on the mechanical scenarios (domestic migration: 0, 0.5, 2, international migration: 0 and 1) or SSP-based scenarios (SSP2, SSP3 and SSP5).

- First_Pop_Add.R (Modifies output)
  - Adds the base-year population to the projected population table that starts with 2011. After running
  this script, the resulting population table starts with 2010.

- State_Level_Migration.R (Generates additional output)
  - Generates two dataframes for each state, one migration numbers from all other states to the current state,
  and the other migration from the current state to all other states.

- Regional_Migration_Vis.R (Generates additional output)
  - Visualizes projected regional migration from each region (south, west, midwest, northeast) to the others
  based on migration numbers of the constituent states of each region.  

- Pop_Dis_Vis.R (Generates additional output)
  - Visualizes contributing factors to U.S. state-level population. For each state, it disaggregates population change
  between 2010 and 2050 based on the contributing factors, which are fertility/mortality/age structure, international
  migration and domestic migration.

- National_Pop_Vis_IIASA.R (Generates additional output)
  - Compares and visualizes the national-level population projections from our bilateral model to the equivalent
  projections from IIASA.

- Fert_Int_Mig.R (Generates additional output)
  - Calculates the national-level international migration and TFR estimates for the U.S. over all projection years
  under a specified scenario by utilizing projected state-level TFR and international migration values.

- Single_State_Vis.R (Generates additional output)
  - For each state, it plots population change from 2010 to 2100 based on assuming no migration, only international
  migration and both international and domestic migration.

- state_level_pop_vis.py (Generates additional output)
  - This is a script for displaying maps of state-level population growths according to
  different scenarios using our multi-state population projection model.

- state_level_upscaled_vis.py (Generates additional output)
  - This is a script for displaying maps of state-level population growths according to
  different SSPs by upscaling national-level population grids to state boundaries.

# Package
In addition to standard packages necessary to run our scripts, We also have an R package, called multistate
that can be installed using the source file multistate_0.1.0.tar.gz. This package includes functions that
we wrote for our analysis.
