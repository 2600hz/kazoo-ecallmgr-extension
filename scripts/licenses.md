# Licenses

The license system implemented in ecallmgr-extension restricts access to the applications unless the system_config/licenses document contains the necessary values to allow it.  The current algorithm, called 'prime', is very rudimentary and should be consider a barrier to entry as well as legal instrument.   

### Generation
In the [scripts](https://github.com/2600hz/kazoo-ecallmgr-extension/tree/master/scripts) directory a bash script is provided to generate licenses for ecallmgr-extension.  

**NOTE: This script contains the 'prime' algorithm as well as the secrets used to create licenses and should be considered hugely sensitive.  Distribution is strictly prohibited!**

The script is not part of any deployments and requires the directory structure of the source repository to work properly.  The parameters can be provided as arguments to the script and if any are missing a 'dialog' wizard will be invoked to collect them interactively.

#### Generation: Prerequisites
* On any application server in the cluster that you are generating licenses for, get the Master Account ID.  To get this via SUP commands follow these instructions (the bold text below is an example Master Account ID returned):
	* `sup kapps_util get_master_account_id`
		* {ok,<<"**2747f5c11e978a288c3aee0c789d5074**">>}
* On any linux host under your direct control (never clone to a client or remote server) clone the [ecallmgr-extension repository](https://github.com/2600hz/kazoo-ecallmgr-extension)
	* `git clone git@github.com:2600hz/kazoo-ecallmgr-extension.git`
* Change to the clone directory
	* `cd  kazoo-ecallmgr-extension`

#### Generation: Creating Licenses via the Wizard
* Run the license generation script on your local linux host.  From within the directory you cloned the ecallmgr-extension repository to (above):
	* `scripts/licenses.sh`
* Enter the Master Account ID you retrieved prior then hit tab until 'Next' is highlighted, then the enter key
* Set the expiration date for the licenses to be generated, use tab to move around.  When complete hit tab until 'Next' is highlighted, then the enter key
	* By default the tool will generate licenses for a one year period
* Us the up and down arrow keys to highlight features, pressing spacebar will place or remove an asterisk next to the selected feature.  All features with asterisks will be licensed.  When all selections have been made hit tab until 'Generate' is highlighted, then the enter key

The tool will provide a partial JSON payload that will need to be added to system_config on the cluster to be licensed.  Additionally, it will write the same payload to `/tmp/ecallmgr_extension-{ACCOUNT_ID}-{EXPIRATION}.licenses` for use by the install tool.

#### Generation: Creating Licenses via Arguments
Run the license generation script on your local linux host.  From within the directory you cloned the ecallmgr-extension repository to (above):

`scripts/licenses.sh -a <master account id> -e <expiration as YYYY-MM-DD> -f <feature name to enable> -f <feature name to enable>`

**NOTE: if the -e (expiration) argument is not provided it will default to one year from the date the license is generated!**

If any parameters are not provided the wizard will be loaded using the provided arguments as defaults.

To get a list of available feature names use:
`scripts\licenses.sh -l`

### Managing Licenses
Once you have created licenses you must install them in the database of the cluster to be licensed and renew them prior to expiration.

#### Managing Licenses: Adding multiple Licenses via SUP
When generating licenses a `.licenses` file is created in the `/tmp` directory of the server running the generation tool.  Use `scp` or other means to move this file to a server in the cluster to be licensed that is running ecallmgr-extension.  In our example we will place the file `ecallmgr_extension-2747f5c11e978a288c3aee0c789d5074-63739008000.licenses` in the `/root/` directory.

Execute the following command:
* `sup ecallmgr_extension_maintenance install_licenses /root/ecallmgr_extension-2747f5c11e978a288c3aee0c789d5074-63739008000.licenses`

For each license in the file you should see an output similar to checking licenses via sup.

#### Managing Licenses: Adding single License via SUP
It is possible to use SUP to add a single license using copy and paste.  You must have the following information from the generation tool:
* Feature Name
* Expiration
* License

Using this information run the following command:
* `sup ecallmgr_extension_maintenance install_license {Feature Name} {Expiration} {License}`

You should see an output similar to checking licenses via sup.

#### Managing Licenses: Direct to Database
Connect to the database UI and navigate to the `system_config` database.  Switch to the `licenses` document.  If there is no such document, click create new document and replace the generated document `_id` with the word `licenses`.

If the document exists or you just created it in the `default` object ensure an object `ecallmgr_extension` exists and add the output from the license generation tool there.  For example:

    {
      "_id": "licenses",
      "default": {
        "ecallmgr_extension": {
          "application": {
            "license": "2a19e60467b5e6785428ac6e9cec7649531e950bc3f87afda6c910f42bf78e52",
            "type": "prime",
            "expiration": 63739008000
          }
        }
      }
    }

This is a standard system_config document, so the same rules for node specific configuration applies.
  
#### Managing Licenses: Checking Licenses via SUP

Running the following SUP command will verify all installed licenses:
* `sup ecallmgr_extension_maintenance check_licenses`

This will provide one of two outputs for each license.  If successful you will get:
* `valid license for {FEATURE NAME} until {DATE}` 
otherwise you will get the error:
* `WARNING: license for {FEATURE NAME} is invalid`

