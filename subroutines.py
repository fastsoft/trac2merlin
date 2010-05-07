#!/usr/bin/env python

# Subroutines for trac2merlin extractor

def de_unicodify_rows(r):
    """
    Converts sqlite3 rows into ascii from unicode.
    """
    def du(s):
        if isinstance(s,unicode): return str(s)
        else: return s
    return [
        dict(
            (du(key), du(value)) 
            for key,value in row.items()
        )
        for row in r
    ]

def get_milestone_names(con):
    "Returns a list of milestone names."
    query = "select name from milestone;"
    res = con.execute(query)
    milestone_names = [str(r['name']) for r in res.fetchall()]
    return milestone_names

def get_usermap(con):
    "Return a dictionary of usernames mapping to real names."
    query = "select sid, value from session_attribute where name='name';"
    res = con.execute(query)
    usermap = dict([(str(x['sid']),str(x['value'])) for x in res.fetchall()])
    return usermap

milestone_query = """
SELECT 
   id,
   type,
   time,
   changetime,
   component,
   severity,
   priority,
   owner,
   reporter,
   version,
   milestone,
   status,
   resolution,
   summary,
   keywords
  FROM ticket
  WHERE milestone = '%s'
        AND keywords like '%%UpdateAccepted%%'
  ORDER BY id
"""

def get_milestone_tickets(milestone_name, con):
    "Returns a list of ticket dictionaries for the given milestone."
    res = con.execute(milestone_query % milestone_name)
    r = res.fetchall()
    if len(r) != 0:
        r = de_unicodify_rows(r)
    return r


custom_map = {
    'dev_start_date': 'dev_start_date',
    'dev_progress': 'dev_progress',
    'dependencies': 'dependencies',
    'time_estimate': 'time_estimate',
    'progress': 'Stage',
    'qa_start_date': 'qa_start_date',
    'qa_progress': 'qa_progress',
    'qa_time_estimate': 'qa_time_estimate',
}
custom_map_reverse = dict((value,key) for key,value in custom_map.items())
def add_custom_fields(ticket, con):
    """
    Modify the ticket dictionary in place to include the custom fields.
    """
    id = ticket['id']

    # INSERT CUSTOM FIELDS
    query = "select * from ticket_custom where ticket = %d" % id
    result = con.execute(query)
    custom_rows = de_unicodify_rows(result.fetchall())

    # Make sure everybody has the mapped fields
    for key,value in custom_map.items():
        ticket[value] = ''

    # Insert the field into the main row if it matches the map above
    for custom_row in custom_rows:
        name = custom_row['name']
        if name in custom_map:
            merlin_name = custom_map[name]
            ticket[merlin_name] = custom_row['value']
            #print 'name %s -> merlin_name %s' % (name, merlin_name)


def find_time_ranges(ticket, target_field, con):
    """
    Goes through a ticket's changes and returns a dictionary keyed on each state
    that occupied the target field and valued on a list of xranges that describe 
    what times the target field was in each state.
    """

    # Database fetch for changes
    id = ticket['id']
    query = "select * from ticket_change where ticket = %d order by time desc;" % id
    result = con.execute(query)
    changes = result.fetchall()
    
    # Initial values
    try:
        # Normal field
        oldstate = newstate = str(ticket[target_field])
    except:
        # Custom field (have to fetch initial value from custom table)
        query = "select * from ticket_custom where ticket = %d and name = '%s';"
        query = query % (id, target_field)
        customs = con.execute(query).fetchall()
        oldstate = newstate = str(customs[0]['value'])

    end = begin = ticket['changetime'] + 1
    state_ranges = {}
    
    # Loop through changes backwards in time (see the above query)
    for change in changes:
        #print '/',target_field,'/',change['field'],'/',change['time']
        if target_field == str(change['field']):
            #print 'change', change
            begin    = change['time']
            newstate = str(change['oldvalue'])
            newrange = xrange(begin,end)

            if oldstate not in state_ranges:
                state_ranges[oldstate] = [newrange]
            else:
                state_ranges[oldstate].append(newrange)

            #print 'state_ranges', state_ranges
        end = begin
        oldstate = newstate

    #print 'oldstate:',oldstate
    #print 'ticket', ticket
    # Now finish off the very first state.
    begin    = ticket['time']
    newrange = xrange(begin,end)
    if oldstate not in state_ranges:
        state_ranges[oldstate] = [newrange]
    else:
        state_ranges[oldstate].append(newrange)
    #print 'state_ranges', state_ranges

    return state_ranges

def get_transition_time(ticket_id, target_field, initial_value, final_value, con):
    """
    Returns the latest timestamp at which a transition from one of the initial_values
    to one of the final_values occured. Supply and empty list or None to match all
    initial or final values.
    """

    # Coerce the singleton args into lists if possible
    def c(arg):
        if type(arg) == str:
            return [arg]
        else:
            try:
                return list(arg)
            except:
                return [arg] if arg else []
    initial_values = c(initial_value)
    final_values = c(final_value)

    # Database fetch for changes
    query = """
    select * from ticket_change
    where ticket = %d 
        %s%s%s
    order by time desc limit 1;
    """ % (
        ticket_id, 
        "and field = '%s'\n" % target_field
            if target_field
            else '', 
        "        and (%s)\n" % ' or '.join("oldvalue = '%s'" % f for f in initial_values) 
            if len(initial_values) 
            else '',
        "        and (%s)\n" % ' or '.join("newvalue = '%s'" % f for f in final_values) 
            if len(final_values) 
            else '',
    )
    #print 'query', query
    result = con.execute(query)
    changes = result.fetchall()
    #print 'changes', changes
    if len(changes):
        return changes[0]['time']
    else:
        return None
        

def number_in_xrange(n,r):
    """
    True if a number falls within the xrange object (inclusive).

    Examples:

    >>> number_in_xrange(0,xrange(0,1))
    True
    >>> number_in_xrange(1,xrange(0,1))
    True
    >>> number_in_xrange(2,xrange(0,1))
    False
    """
    return n >= r[0] and n <= r[-1] + 1


def xrange_overlap(a,b):
    """
    Returns the xrange overlap region of two xranges.

    Examples:

    >>> xrange_overlap(xrange(0,1),xrange(1,2))
    xrange(1, 1)
    """

    start = None
    end = None
    try:
        if number_in_xrange(a[0], b):
            start = a[0]
        elif number_in_xrange(b[0], a):
            start = b[0]

        if number_in_xrange(b[-1]+1, a):
            end = b[-1]+1
        elif number_in_xrange(a[-1]+1, b):
            end = a[-1]+1

        if start and end:
            return xrange(start, end)
        else:
            return None

    except IndexError, e: # If the xrange is zero length
        return None




def _test():
    import doctest
    doctest.testmod()
if __name__ == "__main__":
    _test()

