
from __future__ import print_function

from dbh import DbHandler

def main():
    h = DbHandler()

    events = h.select_from_where('marker_event_items', '*', where={'dateOfCreation': ('>=', 1530540370), 'id': 23999, 'clientDeviceId': ('like', 'web_u%')})

    for ev in events:
        print(ev)

if __name__ == '__main__':
    main()

