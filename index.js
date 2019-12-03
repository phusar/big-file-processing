'use strict';
const fs = require('fs');
const es = require('event-stream');

const data = {
    lines: 0,
    names: [],
    commonNames: {},
    dates: {}
}

const stream = fs.createReadStream('./itcont.txt')
    .pipe(es.split())
    .pipe(es.mapSync(function(line){
        stream.pause();
        const columns = line.split('|');
        let name = columns[7];
        const date = columns[4];
    
        data.lines++;
        // We don't want to store all the millions of names in memory, just get #233 and #33243, let's just store those two names instead
        if (data.lines === 233 || data.lines === 33243) {
            data.names.push(name);
        }
        
        const firstName = parseFirstName(name);
        if (!data.commonNames[firstName]) {
            data.commonNames[firstName] = 1;
        } else {
            data.commonNames[firstName]++;
        }

        processDate(data, date);
        stream.resume();
    })
    .on('error', function(err){
        console.log('Error while reading file.', err);
    })
    .on('end', function(){
        console.log(`Total number of lines: ${data.lines}`);
        console.log(`Name at position #233: ${data.names[0]}`); // Arrays in JS start at zero, hence 232 instead of 233
        console.log(`Name at position #33243: ${data.names[1]}`);
        printDonationsPerMonth(data.dates);
        const mostCommonName = getMostCommonName(data.commonNames);
        console.log(`Most common name is: ${mostCommonName.name} with ${mostCommonName.count} occurences.`);
    })
);

function parseFirstName(name) {
    if (!name) {
        // Some entries do not have a name, let's make it a valid string
        return 'ANONYMOUS';
    }
    // Not all names are in the same format
    const firstNameArray = name.match(/(?<=,)([A-Z'-]+)\w+|(?<=. |\.)([A-Z'-]+)\w+/); 
    // If we couldn't parse it, just use the name as it is
    return firstNameArray && firstNameArray[0] || name; 
}

function processDate(data, date) {
    if (!date) {
        return;
    }
    const year = date.substr(0, 4);
    const month = date.substr(4, 2);
    if (!data.dates[year]) {
        data.dates[year] = getEmptyYear();
    }
    data.dates[year][month]++;    
}

function getMostCommonName(names) {
    const mostCommon = {
        name: undefined,
        count: 0
    };
    for (let name in names) {
        if (names[name] > mostCommon.count) {
            mostCommon.name = name;
            mostCommon.count = names[name];
        }
    }
    return mostCommon;
}

function printDonationsPerMonth(dates) {
    console.log('Donations per month:');
    for (let year in dates) {
        console.log(`${year}:`);

        // Sort the months so we get the output formatted in a human-readable way
        const sortedKeys = Object.keys(dates[2019]).sort();
        for (let month of sortedKeys) {
            console.log(`   ${month}: ${dates[year][month]}`);
        }
    }
}

// This will also prevent a situation where a month will be missing in the output, in case there aren't any donations that month
function getEmptyYear() {
    return {
        '01': 0,
        '02': 0,
        '03': 0,
        '04': 0,
        '05': 0,
        '06': 0,
        '07': 0,
        '08': 0,
        '09': 0,
        '10': 0,
        '11': 0,
        '12': 0
    };
}