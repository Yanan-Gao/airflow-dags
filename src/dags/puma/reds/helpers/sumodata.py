class SumoData:

    def __init__(self, filepath, last_accessed_datetime, account_id, principal_id, workflow_type=0):
        self.feed_id = None
        self.filepath = filepath
        self.last_accessed_datetime = last_accessed_datetime
        self.account_id = account_id
        self.principal_id = principal_id
        # reds = 0, exposure = 1
        self.workflow_type = workflow_type

    def set_feed_id(self, feed_id):
        self.feed_id = feed_id
