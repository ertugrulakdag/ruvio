#!/bin/bash

# ceiling_decide PREV_RPS CURR_RPS CURR_SERVER_CORE_PERCENT
# saturated: this point fills one core and is the candidate.
# plateau: this point gained under 5%, so the previous point is kept.
# continue: keep walking.
ceiling_decide() {
    awk -v prev="$1" -v curr="$2" -v cpu="$3" 'BEGIN {
        if (cpu + 0 >= 90) { print "saturated"; exit }
        if (curr + 0 < prev * 1.05) { print "plateau"; exit }
        print "continue"
    }'
}

ceiling_file_status() {
    if [[ "$1" == "saturated" && "$2" == "saturated" && "$3" == "saturated" ]]; then
        printf '%s\n' saturated
    else
        printf '%s\n' client-bound
    fi
}
