from collections import Counter


def set_default_map_for_map(map, key):
    if key not in map:
        map[key] = {}
    return map[key]


def set_default_set_for_map(map, key):
    if key not in map:
        map[key] = set()
    return map[key]


def set_default_counter_for_map(map, key):
    if key not in map:
        map[key] = Counter()
    return map[key]


class PlayerIdMap():
    # 按平台，渠道,日期划分playerId的set
    def __init__(self):
        self.player_id_map = {}

    def __len__(self):
        return len(self.player_id_map)

    def put(self, time_str, platform, channel, player_id):
        channels = set_default_map_for_map(
            self.player_id_map, platform)
        days = set_default_map_for_map(channels, channel)
        player_ids = set_default_set_for_map(days, time_str)
        player_ids.add(player_id)

    def get_total_player_ids(self):
        ret = {}
        for platform, channels in self.player_id_map.items():
            platform_map = set_default_map_for_map(ret, platform)
            for channel, days in channels.items():
                player_ids = set_default_set_for_map(platform_map, channel)
                for _, ids in days.items():
                    player_ids.update(ids)
        return ret

    def get_days(self):
        ret = set()
        for platform, channels in self.player_id_map.items():
            for channel, days in channels.items():
                for day, ids in days.items():
                    ret.add(day)
        return ret

    def get_some_day_player_ids(self, platform, channel, time_str):
        channels = self.player_id_map.get(platform, {}).get(channel, {})
        if len(channels) > 0:
            if time_str in channels:
                ids = channels.get(time_str)
                if len(ids) > 0:
                    return ids, True
        return set(), False

    def get_all_day_player_ids(self, platform, channel):
        ret = set()
        channels = self.player_id_map.get(platform, {}).get(channel, {})
        if len(channels) > 0:
            for _, ids in channels.items():
                ret.update(ids)
            return ret, True
        return ret, False

    def size(self):
        ret = 0
        for platform, channels in self.player_id_map.items():
            for channel, days in channels.items():
                for day, ids in days.items():
                    ret = ret + len(ids)
        return ret

    def get_total_player_ids_counter(self):
        ret = {}
        for platform, channels in self.player_id_map.items():
            platform_map = set_default_map_for_map(ret, platform)
            for channel, days in channels.items():
                player_ids = set_default_counter_for_map(platform_map, channel)
                for _, ids in days.items():
                    player_ids.update(ids)
        return ret

    def get_all_day_player_ids_counter(self, platform, channel):
        ret = Counter()
        channels = self.player_id_map.get(platform, {}).get(channel, {})
        if len(channels) > 0:
            for _, ids in channels.items():
                ret.update(ids)
            return ret, True
        return ret, False
