#!/bin/bash

###
### Store the current working directory
###
pushd "$(dirname "$0")" >/dev/null

###
### Ensure all paths are relative to
### the scripts location, not where
### the script is invoked from.
###
ROOT="$(pwd -P)"/..
cd $ROOT

###
### The static license generation
### parameters.
###
salt='7f1d655ebb494a2a8773554b551e2eb7'
app_name='ecallmgr_extension'
options=(application "" off)
#options+=(feature_1 "" off)
#options+=(feature_2 "" off)

###
### Internal variables that can come from
### arguments to the script or the dialog
### screen if required
###
account_id=""
expiration=""
features=()

###
### Proceess script `arguments
###
while getopts a:e:f:lh option
do
case "${option}"
in
a) account_id=${OPTARG};;
e) expiration=$(date +'%d %m %Y' -d"$OPTARG");;
f) features+=(${OPTARG});;
l) list=1 ;;
h) show_help=1 ;;
esac
done

###
### Check that all provided features are 
### in the options array as well as set
### the option to 'on' to enable it if
### the dialog is shown
###
for feature in "${features[@]}"; do
    options_length=${#options[@]}
    found=""
    for (( i=0; i<${options_length}; i=i+3 )); do
        if [ "${options[$i]}" == "${feature}" ]; then
            options[$i+2]="on"
            found=1
        fi
    done
    if [ -z $found ]; then
        >&2 echo "ERROR: invalid feature '$feature'"
        invalid_features=1
    fi
done

###
### Stop the script if an invalid feature
### was provided, error was printed above.
###
if [ -n "$invalid_features" ]; then
    exit 1
fi

###
### If no expiration is provided default
### to one year from the current date.
###
if [ -z "$expiration" ]; then
    expiration=$(date +'%d %m %Y' -d'next year')
fi

###
### If the help argument is provided
### don't do anything but show the
### help text.
###
if [ -n "$show_help" ]; then
    echo "Usage: license.sh [OPTION]..."
    echo "Geneartes a license for the $app_name application."
    echo "Argument short options."
    echo "  -l			List all available feature names and exit"
    echo "  -h			This help ;)"
    echo "  -a=account_id		Master Account ID"
    echo "  -e=YYYY-MM-DD		Expiration"
    echo "  -f=feature_name	Enable a feature, can be repeated for multiple features"
    echo
    echo "If -a and -f arguments are provided the on-screen wizard will be skipped"
    echo "and the license generated automatically.  If not enough parameters"
    echo "are provided to generate the license the wizard will be invoked and"
    echo "any arguments used as defaults."
    echo "If the expiration is not provided it will default to one year from"
    echo "when the script is executed."
    exit 1

###
### If the list argument is provided
### print a list of all possible features
###
elif [ -n "$list" ]; then
    options_length=${#options[@]}
    for (( i=0; i<${options_length}; i=i+3 )); do
        echo ${options[$i]}
    done
    exit 0

###
### If there is not enough information
### provided in the arguments to generate
### a license file, show in interactive
### dialog 'wizard'.
###
elif [ -z "$account_id" ] || [ ${#features[@]} -eq 0 ]; then
    ###
    ### Build the dialog command
    ###
    cmd=(dialog
         --backtitle "${app_name^} License Generator (prime)"
         --ok-label "Next"
         --nocancel
         --title "Step 1 of 3"
         --inputbox "Master Account ID:" 0 76 $account_id
         --and-widget
         --separate-widget ';'
         --ok-label "Next"
         --nocancel
         --title "Step 2 of 3"
         --calendar "Expiration:" 0 76 $expiration
         --and-widget
         --separate-widget ';'
         --ok-label "Generate"
         --nocancel
         --title "Step 3 of 3"
         --checklist "Select features:" 22 76 16
        )

    ###
    ### Use the command above to show a dialog
    ### but append the options (features) that
    ### can be selected and collect the responses
    ### in the 'choices' array.
    ###
    choices=($("${cmd[@]}" "${options[@]}" 2>&1 >/dev/tty))

    ###
    ### The dialog command is complete
    ### clear the screen so we can
    ### print the json on an empty canvas
    ###
    clear

    ###
    ### Seperate choices by ';' then make
    ### it into an array so that the account_id
    ### and the expiration are ALWAYS the first
    ### two elements
    ###
    IFS=';' read -ra choices <<< "${choices[@]}"
 
    ###
    ### The first element is the account_id
    ###
    account_id=${choices[0]}

    ###
    ### The expiration is the second element
    ### but needs to be reformated to m/d/y
    ###
    expiration=$(echo -n ${choices[1]} | awk -F '/' '{print $2 "/" $1 "/" $3}')

    ###
    ### All other choices are the selected features
    ###
    features=("${choices[@]:2}")
else
    ###
    ### If we don't show the dialog screen
    ### reformat the expiration to m/d/y
    ### from 'd m y' format that dialog needed
    ###
    expiration=$(echo -n ${expiration} | awk '{print $2 "/" $1 "/" $3}')
fi


###
### If account_id is still empty
### show an error
###
if [ -z "$account_id" ]; then
    >&2 echo "ERROR: no account id provided"
fi

###
### If expiration is still empty
### show an error otherwise reformat
### the date to gregorian seconds
###
if [ -z "$expiration" ]; then
    >&2 echo "ERROR: invalid expiration provided"
else
    expiration=$(($(date +%s -d"$expiration") + 62167219200))
fi

###
### If there are no features selected
### show an error
###
if [ ${#features[@]} -eq 0 ]; then
    >&2 echo "ERROR: no features selected"
fi

###
### If we don't have enough information
### to create a license file end the script,
### errors should have been printed above.
###
if [ -z "$account_id" ] || [ -z "$expiration" ] || [ ${#features[@]} -eq 0 ]; then
    exit 1
fi

###
### Start a JSON object then loop the
### selected features and create the
### license parameters for them
###
json="{\"$app_name\":{"
for feature in "${features[@]}"; do
    json+="\"$feature\":{\"license\":\""
    json+=$(echo -n "prime:$salt:$account_id:$salt:$feature:$salt:$expiration" | sha256sum | awk '{print $1}')
    json+="\", \"expiration\":$expiration, \"type\":\"prime\"},"
done
json=${json::-1}
json+='}}'

###
### Print the result to the screen and
### a file in the /tmp directory
###
echo $json | python -m json.tool
echo $json > "/tmp/$app_name-$account_id-$expiration.licenses"

###
### Restore the working directory
###
popd >/dev/null
