#!/bin/bash
set -ex  # For debugging the script

# Determine directories
SCRIPT_DIR=$(dirname $(realpath $0))
MAVEN_DIR=$(cd $SCRIPT_DIR/../../../ && pwd)
DB_SCRIPT_DIR=$(cd $SCRIPT_DIR/db-scripts && pwd)

# Navigate to the Maven project directory
cd "$MAVEN_DIR" || exit

# Run the Maven clean package command
if mvn clean package; then
    echo "First script completed successfully. Proceeding to the second script."

    # Run the Maven compile command
    if mvn compile; then
        echo "Second script completed successfully."
        
        # Navigate back to the script directory
        cd "$SCRIPT_DIR" || exit
        
        # Run the third script
        if ./shopingcart-setup-initial-data.sh; then
            echo "Third script completed successfully."

            # Run the fourth script
            if ./initialize-cassandra-db.sh; then
                echo "Fourth script completed successfully."

                # Change directory for the fifth script
                cd "$DB_SCRIPT_DIR" || { echo "Failed to change directory to $DB_SCRIPT_DIR"; exit 1; }

                # Run the fifth script
                if ./copy-shop_db-file-docker.sh; then
                    echo "Fifth script completed successfully."

                    # Run the sixth script
                    if ./create-shop_db-schema.sh; then
                        echo "Sixth script completed successfully."

                        # Run the seventh script
                        if ./load-shop_db-data.sh; then
                            echo "Seventh script completed successfully."
                        else
                            echo "Seventh script failed."
                            exit 1
                        fi

                    else
                        echo "Sixth script failed."
                        exit 1
                    fi

                else
                    echo "Fifth script failed."
                    exit 1
                fi

            else
                echo "Fourth script failed."
                exit 1
            fi

        else
            echo "Third script failed."
            exit 1
        fi

    else
        echo "Second script failed."
        exit 1
    fi

else
    echo "First script failed. Exiting."
    exit 1
fi
