from collections import Counter


class LoginsByDayAndCounter:
    login_counter = Counter()
    login_group_by_day = {}

    def __len__(self):
        return len(self.login_counter)

    def update(self, player_id):
        self.login_counter.update([player_id])

    def put(self, time_str, player_id):
        player_ids = self.login_group_by_day.get(time_str, set())
        player_ids.add(player_id)
        self.login_group_by_day[time_str] = player_ids
