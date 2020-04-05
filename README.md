# MarkUp

*   [Overview](#overview)
*   [Step 1: Environment setup](#step-1-environment-setup)
*   [Step 2: Cloud environment setup](#step-2-cloud-environment-setup)
*   [Step 3: Configure Data Sources](#step-3-configure-data-sources)
*   [Step 4: Create Data-Studio Dashboard](#step-4-data-studio-dashboard)

## Overview

MarkUp solution is built for Google Shopping customers to take actionable
data-driven decisions to improve their feed health and shopping ads performance.

## Step 1: Environment setup

1.  [Create a GCP account](https://cloud.google.com/?authuser=1) (if you don't
    have one already!)

2.  [Create a new project](https://console.cloud.google.com/cloud-resource-manager)

    *   Click Create Project.
    *   In the New Project window that appears, enter a project name and select
        a billing account as applicable.
    *   When you're finished entering new project details, click Create.

3.  [Open cloud shell](https://console.cloud.google.com/cloudshell) and clone
    the repository.

    ```
      git clone https://cse.googlesource.com/solutions/markup
    ```

## Step 2: Cloud environment setup

1.  Perform environment setup after providing inputs.

    *   `project_id`:
        [GCP Project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
    *   `merchant_id`:
        [Google Merchant Center Id](https://support.google.com/merchants/answer/188924?hl=en)
    *   `customer_id`:
        [Google Ads External Customer Id](https://support.google.com/google-ads/answer/1704344?hl=en)

    ```
      python3 cloud_env_setup.py --project_id=<project_id> --merchant_id=<merchant_id> --customer_id=<customer_id>
    ```

## Step 3: Configure Data Sources

1.  [Create Product Detailed Data Source](https://datastudio.google.com/c/u/0/datasources/create?connectorId=2)

    *   Search `<project_id>` under My Projects
    *   Under Dataset, click on the "`markup`"
    *   Under Table, choose "`product_aggregated_category`"
    *   Click Connect on the top right corner and wait for the data-source to be
        created

2.  [Create Product Aggregated Category Data Source](https://datastudio.google.com/c/u/0/datasources/create?connectorId=2)

    *   Search `<project_id>` under My Projects
    *   Under Dataset, click on the "`markup`"
    *   Under Table, choose "`product_aggregated_category`"
    *   Click Connect on the top right corner and wait for the data-source to be
        created

3.  [Create Product Aggregated Label Data Source](https://datastudio.google.com/c/u/0/datasources/create?connectorId=2)

    *   Search `<project_id>` under My Projects
    *   Under Dataset, click on the "`markup`"
    *   Under Table, choose "`product_aggregated_label`"
    *   Click Connect on the top right corner and wait for the data-source to be
        created

## Step 4: Create Data-Studio Dashboard

1.  Open
    [link](https://datastudio.google.com/c/u/0/reporting/1IsvsvrfAvyhefHK33zxfj72neYfn9YnO/page/e377/preview)
2.  Click "`Use Template`"
3.  Choose the new data-sources created in the previous step in the following
    order.
    *   Product Aggregated Category
    *   Product Detailed
    *   Product Aggregated Label
4.  Click "`Copy Report`"
