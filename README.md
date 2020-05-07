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

    *   Create a cookie for the Git client to use by visiting
        [https://cse.googlesource.com/new-password](https://cse.googlesource.com/new-password)
        and following the instructions.
    *   Execute following command.

    ```
      git clone https://cse.googlesource.com/solutions/markup
    ```

## Step 2: Cloud environment setup

1.  Make sure the user executing next step has following permissions.

    *   [Standard Access For GMC](https://support.google.com/merchants/answer/1637190?hl=en)
    *   [Standard Access For Google Ads](https://support.google.com/google-ads/answer/7476552?hl=en)
    *   [Editor(or Owner) Role in Google Cloud Project](https://cloud.google.com/iam/docs/understanding-roles)

2.  Perform environment setup after providing inputs.

    *   `project_id`:
        [GCP Project Id](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
    *   `merchant_id`:
        [Google Merchant Center Id](https://support.google.com/merchants/answer/188924?hl=en)
    *   `ads_customer_id`:
        [Google Ads External Customer Id](https://support.google.com/google-ads/answer/1704344?hl=en)

    During the installation, the script will check whether does current user
    have enough permissions to continue. It may ask you to open cloud
    authorization URL in the browser. Please follow the instructions as
    mentioned in the command line.

    ```
      cd markup
      sh setup.sh --project_id=<project_id> --merchant_id=<merchant_id> --ads_customer_id=<ads_customer_id>
    ```

## Step 3: Configure Data Source

1.  Create `Product Detailed` Data Source

    *   Click on the
        [link](https://datastudio.google.com/c/u/0/datasources/create?connectorId=2)
    *   Make sure you are using BigQuery connector. If not choose "`BigQuery`"
        from the list of available connectors.
    *   Search "`project_id`" under My Projects
    *   Under Dataset, click on "`markup`"
    *   Under Table, choose "`product_detailed`"
    *   Click `Connect` on the top right corner and wait for the data-source to
        be created

## Step 4: Create Data-Studio Dashboard

1.  Click on the
    [link](https://datastudio.google.com/c/u/0/reporting/1IsvsvrfAvyhefHK33zxfj72neYfn9YnO/page/e377/preview)
2.  Click "`Use Template`"
3.  Choose the new "`Product Detailed`" data-source created in the previous step
4.  Click "`Copy Report`"
