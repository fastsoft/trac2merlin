#!/usr/bin/env python
"""
Trac sqlite3 database 2 Merlin converter.
Ryan Witt (onecreativenerd@gmail.com)
"""

import re
import sqlite3
from datetime import datetime
from sys import argv
from pprint import PrettyPrinter
pp = PrettyPrinter(indent=2)
pprint = pp.pprint
from subroutines import *

applescript = False

def dict_factory(cursor, row):
    """
    Feed this to sqlite3.cursor.row_factory in order
    to get rows out as dictionaries.

    Ripped from the sqlite3 docs.
    http://docs.python.org/lib/sqlite3-Connection-Objects.html
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def can_be_int(i):
    try:
        int(i)
        return True
    except: 
        return False
tickets_to_watch = set(
    int(item)
    for item in argv
    if can_be_int(item)
)
print tickets_to_watch

# Get connection
con = sqlite3.connect("trac.db")
con.row_factory = dict_factory

# ASDFASDFASDFASDF
milestone_names = get_milestone_names(con)
usermap         = get_usermap(con)

# Line separation character (files still have to be named .csv for merlin drag 'n drop to work)
SEP_CHAR = ','
NUMBERS = re.compile('[0-9]+')

# Go through and grab tickets for each milestone
milestones = {}
for milestone_name in milestone_names:
    milestone = {}
    print milestone_name

    rows = get_milestone_tickets(milestone_name,con)
    #print rows
    if len(rows) == 0: # Skip milestones with no tickets
        continue

    # Keep track of which line has which ticket (for merlin dependency remap)
    ticket_index = {}
    ticket_parents = {}

    qa_tickets = []

    # EXAMINE ROW DATA
    for row,i in zip(rows,range(len(rows))):
        
        ID = int(row['id'])
        #print ID, ID in tickets_to_watch
        if ID in tickets_to_watch:
            print 'row_before'
            pprint(row)

        id = ID

        # DETERMINE OWNERSHIP PERIODS
        owner_reigns = find_time_ranges(row,'owner',con)
        stage_reigns = find_time_ranges(row,'progress',con)

        # FIND PERSON/STAGE OVERLAPS
        overlaps = dict(
            (owner, dict(
                (stage, [
                    (range, len(range))
                        for range in (
                            xrange_overlap(stage_range, owner_range)
                            for stage_range in stage_ranges
                            for owner_range in owner_ranges
                        )
                        if range is not None
                    ] 
                )
                for stage,stage_ranges in stage_reigns.items()
                if any(
                        xrange_overlap(stage_range, owner_range)
                        for stage_range in stage_ranges
                        for owner_range in owner_ranges
                    )
                )
            )
            for owner,owner_ranges in owner_reigns.items()
        )
        if ID in tickets_to_watch:
            print 'overlaps', overlaps

        
        # FIND DEVS AND QAS
        dev_stages = set([
            #u'0 Not Started',
            #u'1 Under Investigation',
            #u'2 Problem Verified',
            #u'3 Development',
            #u'4 Dev Testing',
            '0 Not Started',
            '1 Under Investigation',
            '2 Problem Verified',
            '3 Development',
            '4 Dev Testing',
        ])
        dev_times = dict(
            (
                person,
                sum(
                    overlaps[person][stage][0][1]
                    for stage in overlaps[person]
                )
            )
            for person in overlaps
            if any(stage in dev_stages for stage in overlaps[person])
        )
        dev_total = sum(t for t in dev_times.values())
        dev_percents = dict(
            (
                person,
                int(100.0*t/dev_total)
            )
            for person,t in dev_times.items()
            if person in usermap
        )
        if ID in tickets_to_watch:
            print 'dev_percents', dev_percents

            
        qa_stages = set([
            #u'5 Waiting for PV',
            #u'6 PV Testing',
            #u'7 Fix Verified',
            '5 Waiting for PV',
            '6 PV Testing',
            '7 Fix Verified',
        ])
        qa_times = dict(
            (
                person,
                sum(
                    overlaps[person][stage][0][1]
                    for stage in overlaps[person]
                )
            )
            for person in overlaps
            if any(stage in qa_stages for stage in overlaps[person])
        )
        qa_total = sum(t for t in qa_times.values())
        qa_percents = dict(
            (
                person,
                int(100.0*t/qa_total)
            )
            for person,t in qa_times.items()
            if person in usermap
        )
        if ID in tickets_to_watch:
            print 'qa_percents', qa_percents

        # Need to also get custom fields from the db
        add_custom_fields(row,con)

        # Prepare index for remapping the dependencies so merlin can understand them
        # This entails rewriting the ticket numbers to row indicies
        ticket_index[id] = i
        if 'dependencies' in row and len(row['dependencies']) != 0:
            pred = str(row['dependencies'])
            deplist = NUMBERS.findall(pred)
            ticket_parents[id] = deplist
            if ID in tickets_to_watch:
                print deplist

        # Dates
        dev_start = get_transition_time(ID,'progress', None, dev_stages, con)
        dev_end = get_transition_time(ID,'progress', dev_stages, qa_stages, con)
        qa_start = dev_end
        qa_end = get_transition_time(ID,'status', None, 'closed', con)
        if ID in tickets_to_watch:
            print 'stdn dev_start', dev_start, 'dev_end', dev_end
            print 'stdn qa_start', qa_start, 'qa_end', qa_end 

        def datify(d):
            return datetime.fromtimestamp(d).strftime('%m/%d/%y') if d else datetime.now().strftime('%m/%d/%y')

        last_progress_update = get_transition_time(ID, 'dev_progress',None,None,con)
        last_change_update   = get_transition_time(ID,None,None,None,con)
        if ID in tickets_to_watch:
            print 'LAST last progress update', last_progress_update
            print 'LAST last change update  ', last_change_update

        last_actuals_reporting_date = ''
        if dev_end:
            row['%_Complete'] = '100%'
            last_actuals_reporting_date = datify(last_progress_update) if last_progress_update else ''
        today = datetime.now().strftime('%m/%d/%y')

        priomap = {
            'highest' : 'Very High',
            'high'    : 'High',
            'normal'  : 'Normal',
            'low'     : 'Low',
            'lowest'  : 'Very Low'
        }

        # Take care of closed tickets
        close_date = ''
        if row['status'] == 'closed':
            close_date = datify(get_transition_time(ID,'status',None,'closed',con))
            row['dev_progress'] = '100%'
            row['qa_progress'] = '100%'
            print 'CLOSED!!!!!',close_date

        if row['dev_progress'] == '100%':
            close_date = datify(dev_end)


        # ===============================================
        # DURING DEV STAGE
        if row['Stage'] in dev_stages:

            try:
                dev_owners =  usermap[row['owner']]
            except:
                dev_owners = ''

            last_actuals_reporting_date = today
            actual_end = ''

        # ===============================================
        # AFTER DEV STAGE
        else:

            # Map-in the users real name if we have it
            dev_owners = ';'.join(
                usermap[d]
                for d in dev_percents.keys()
                if dev_percents[d] > 10
                   and d in usermap
            )

            last_actuals_reporting_date = ''
            actual_end = close_date
        
            # Map-in the users real name if we have it
            qa_owners = ';'.join(
                usermap[q]
                for q in qa_percents.keys()
                if qa_percents[q] > 10
                   and q in usermap
            )

            if row['Stage'] != '7 Fix Verified':
                try:
                    qa_owners =  usermap[row['owner']]
                except:
                    qa_owners = ''

            # Create QA ticket
            qa = {
                'id' : ID,
                '#' : ID,
                'Title' : str(ID) + 'QA - ' + row['summary'],
                '% Complete' : row['qa_progress'],
                'Subtitle' : row['summary'],
                'Priority' : priomap[row['priority']] if row['priority'] in priomap else '', 
                'Assigned Resources' : qa_owners,
                'Given Planned Duration' : row['qa_time_estimate'],
                'Given Planned Earliest Start' : row['qa_start_date'] if 'mm' not in row['qa_start_date'] and len(row['qa_start_date']) else today,
                #'Given Planned Earliest Start' : '',
                #'Actual End' : actual_end,
                'Actual End' : '',
                'Last Actuals Reporting Date' : last_actuals_reporting_date,
                #'Last Actuals Reporting Date' : '',
                #'# Predecessors' : '', # This will be filled in the loop below
            }
            qa_tickets.append(qa)


        if ID in tickets_to_watch:
            print 'row_mid'
            pprint(row)


        # Final mapping (well, almost, one more loop is required later)
        row = {
            'id' : ID,
            '#' : ID,
            'Title' : str(ID) + ' - ' + row['summary'],
            '% Complete' : row['dev_progress'],
            'Subtitle' : row['summary'],
            'Priority' : priomap[row['priority']] if row['priority'] in priomap else '', 
            'Assigned Resources' : dev_owners,
            'Given Planned Duration' : row['time_estimate'],
            'Given Planned Earliest Start' : row['dev_start_date'] if 'mm' not in row['dev_start_date'] and len(row['dev_start_date']) else today,
            'Actual End' : actual_end,
            'Last Actuals Reporting Date' : last_actuals_reporting_date,
            #'# Predecessors' : '', # This will be filled in the loop below
        }

        rows[i] = row
        rows[i] = row
        if ID in tickets_to_watch:
            print 'row_after'
            pprint(row)
        
    for z in qa_tickets:
        rows.append(z)
    print 'qa_tickets ', qa_tickets
    milestone['rows'] = rows
    milestone['ticket_index'] = ticket_index
    milestone['ticket_parents'] = ticket_parents
    milestones[milestone_name] = milestone
    print 'parents', milestone['ticket_parents']
    print 'index', milestone['ticket_index']

print
print

# Loop again to fix parents
for milestone_name, milestone in milestones.items():

    rows = milestone['rows']
    ticket_parents = milestone['ticket_parents']
    ticket_index = milestone['ticket_index']

    print 'ticket_index'
    pprint(ticket_index)

    # Write each row
    for row,i in zip(rows,range(len(rows))):

        ## Remap parents according to merlin's method
        ticket = row['id']
        mapped_parents = []
        #print ticket, ticket_parents
        if ticket in ticket_parents and '# Predecessors' in row:
            #print ticket, 'has parents', ticket_parents[ticket]
            for parent in ticket_parents[ticket]:
                if int(parent) in ticket_index:
                    mapped_parents.append(ticket_index[int(parent)])
            #mp2 = ';'.join([str(2*int(i)+1) for i in mapped_parents])
            mp2 = ';'.join([str(int(i)+1) for i in mapped_parents])
            ##row['#_Predecessors'] = ';'.join([str(2*i+1) for i in mapped_parents])
            row['# Predecessors'] = mp2
            print 'parents for %s: %s -> %s' % (ticket, ticket_parents[ticket], mp2)

# Loop again to print out results
for milestone_name, milestone in milestones.items():

    # Open milestone file
    filename = milestone_name + '.csv'
    print "writing " + filename
    f = file(filename, 'w+')

    rows = milestone['rows']

    # Write header line
    if applescript:
        f.write('tell application "Merlin"\n')
    else:
        f.write(SEP_CHAR.join('"' + str(t) + '"' for t in rows[0].keys()) + '\n')

    # Write each row
    for row,i in zip(rows,range(len(rows))):

        ticket = row['id']
        
        # Write the finished row
        if applescript:
            f.write('set t%d to make new activity\n' % ticket)
            f.write('set title of t%d to "%s"\n' % (ticket,row['Title'])) 
            for key,value in row.items():
                pass
        else:
            row = SEP_CHAR.join(['"' + str(v).replace("'","\'").replace(',',';') + '"' for v in row.values()])
            if ticket in tickets_to_watch:
                print 'TESTROW', row
            f.write(row + '\n')
    
    if applescript:
        f.write('end\n')

