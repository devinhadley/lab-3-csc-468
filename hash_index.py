class HashIndex:
    def __init__(self, relation_name, attribute, num_buckets=10):
        self.relation_name = relation_name
        self.attribute = attribute
        self.num_buckets = num_buckets
        self.buckets = [[] for _ in range(num_buckets)]

    def _hash(self, value):
        return hash(value) % self.num_buckets

    def build(self, relation_config):
        """
        Populate the index from the JSON data.
        Entries are (Value, Page_Index)
        """
        schema = relation_config["schema"]
        attr_idx = schema.index(self.attribute)

        for p_idx, page in enumerate(relation_config["pages"]):
            for record in page:
                val = record[attr_idx]
                b_idx = self._hash(val)
                self.buckets[b_idx].append((val, p_idx))

    def lookup(self, value):
        """
        Returns a list of Page Indices where the value might exist.
        """
        b_idx = self._hash(value)
        bucket = self.buckets[b_idx]

        matching_pages = {p_idx for val, p_idx in bucket if val == value}
        return list(matching_pages)
