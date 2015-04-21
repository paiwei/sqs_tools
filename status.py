import boto.sqs
import optparse
import texttable
from datetime import datetime


class SQSRegion(object):

    def __init__(self, region):
        self.conn = boto.sqs.connect_to_region(region)

    def purge(self, queue_name):
        print 'Purging %s ...' % queue_name
        queue = self.conn.get_queue(queue_name)
        if queue is None:
            print '%s not found.' % queue_name
        else:
            reply = raw_input('Are you sure? (y/n)').lower()
            if reply == 'y':
                queue.purge()
            else:
                print 'Cancel purge.'

    def create(self, queue_name):
        print 'Creating %s ...' % queue_name
        if self.conn.get_queue(queue_name):
            print '%s already exist.' % queue_name
        else:
            if self.conn.create_queue(queue_name):
                print '%s created succussfully.' % queue_name
            else:
                print 'Failed to create %s' % queue_name

    def delete(self, queue_name):
        print 'Deleting %s ...' % queue_name
        queue = self.conn.get_queue(queue_name)
        if queue is None:
            print '%s does not exist.' % queue_name
        else:
            reply = raw_input('Are you sure? (y/n)').lower()
            if reply == 'y':
                if self.conn.delete_queue(queue):
                    print 'Deleted %s successfully.' % queue_name
                else:
                    print 'Failed to delete %s.' % queue_name
                print 'When you delete a queue, the deletion process takes up to 60 seconds.'
            else:
                print 'Cancel deletion.'

    def show_status(self, prefix=''):
        self.queues = self.conn.get_all_queues(prefix=prefix)
        tab = texttable.Texttable()
        header = [
            'Name',
            'Messages Available',
            'Visibility Timeout',
            'Created Time',
            'Last Modified Time'
        ]
        tab.header(header)
        tab.set_cols_align(['l', 'r', 'r', 'c', 'c'])
        tab.set_cols_valign(['m', 'm', 'm', 'm', 'm'])
        tab.set_cols_width([30, 10, 10, 10, 10])
        for queue in self.queues:
            created_time = queue.get_attributes('CreatedTimestamp')['CreatedTimestamp']
            modified_time = queue.get_attributes('LastModifiedTimestamp')['LastModifiedTimestamp']
            row = [
                queue.name,
                queue.count(),
                queue.get_timeout(),
                datetime.fromtimestamp(int(created_time)).strftime('%Y-%m-%d %H:%M:%S'),
                datetime.fromtimestamp(int(modified_time)).strftime('%Y-%m-%d %H:%M:%S')
            ]
            tab.add_row(row)
        print tab.draw()


class SQSStatus(object):

    def __init__(self):
        opt_group = self.setup_option_parser()
        self.options, self.args = opt_group.parse_args()

    def setup_option_parser(self):
        opt_group = optparse.OptionParser()
        opt_group.add_option(
            '--region', '-r', dest='region', action='store', type='string', default='us-west-1',
            help='Set the region.  Default is \'us-west-1\''
        )
        opt_group.add_option(
            '--purge', '-p', dest='purge_queue_name', action='store', type='string', default=None,
            help='Purge all messages in the queue.'
        )
        opt_group.add_option(
            '--create', '-c', dest='create_queue_name', action='store', type='string', default=None,
            help='Create the queue.'
        )
        opt_group.add_option(
            '--delete', '-d', dest='delete_queue_name', action='store', type='string', default=None,
            help='Delete the queue.'
        )
        opt_group.add_option(
            '--status', '-s', dest='status_prefix', action='store', type='string', default=None,
            help='Show all queues started with a given prefix.'
        )
        return opt_group

    def process_region(self, region):
        sqs_region = SQSRegion(region)

        if self.options.status_prefix:
            sqs_region.show_status(self.options.status_prefix)

        if self.options.purge_queue_name:
            sqs_region.purge(self.options.purge_queue_name)

        if self.options.create_queue_name:
            sqs_region.create(self.options.create_queue_name)

        if self.options.delete_queue_name:
            sqs_region.delete(self.options.delete_queue_name)

    def run(self):
        self.process_region(self.options.region)


if __name__ == '__main__':
    SQSStatus().run()
