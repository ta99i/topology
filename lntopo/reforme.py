from .common import DatasetFile
import click
from .parser import ChannelAnnouncement, ChannelUpdate
from tqdm import tqdm
from datetime import datetime
import csv
import os


def append_csv(rows, path):
    header = not os.path.exists(path)

    with open(path, mode='a', newline='') as file:
        writer = csv.writer(file)

        # Write the header if the file is new
        if header:
            writer.writerow(rows[0].keys())

        # Write the rows
        for row in rows:
            writer.writerow(row.values())


@click.group()
def reforme():
    pass


@reforme.command()
@click.argument("dataset", type=DatasetFile())
def reforme(dataset):
    '''
        reforme data set for tvalgoteam 
    '''
    if not os.path.exists("data/channels/"):
        os.makedirs('data/channels/')
    for m in tqdm(dataset, desc="Replaying gossip messages"):
        if isinstance(m, ChannelAnnouncement):
            node1 = min(m.node_ids[0].hex(), m.node_ids[1].hex())
            node2 = max(m.node_ids[0].hex(), m.node_ids[1].hex())
            row = [{'channelId': m.num_short_channel_id,
                    'node1Pub': node1, 'node2Pub': node2}]
            append_csv(row, 'data/dictionary.csv')
        if isinstance(m, ChannelUpdate):
            row = [{
                'lastUpdate': m.timestamp,
                'direction': m.direction+1,
                'timeLockDelta': m.cltv_expiry_delta,
                'minHtlc': m.htlc_minimum_msat,
                'feeRateMilliMsat': m.fee_proportional_millionths,
                'feeBaseMsat': m.fee_base_msat,
                'disabled': m.disable,
                'maxHtlcMsat': m.htlc_maximum_msat

            }]
            append_csv(row, f'data/channels/{m.num_short_channel_id}.csv')
