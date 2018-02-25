import asyncio
import os
import argparse
import zmq
import zmq.asyncio
import json
import logging
import time
import traceback
import re
import sys
import csv
from zmapi.codes import error
import aiohttp
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime
from datetime import datetime
from zmapi.zmq.utils import *
from zmapi.connector.controller import RESTController, ControllerBase
from zmapi.logging import setup_root_logger, disable_logger
from collections import defaultdict

################################## CONSTANTS ##################################

CAPABILITIES = sorted([
    "GET_SNAPSHOT",
    "LIST_DIRECTORY",
])

MODULE_NAME = "ddex.io-md"

################################ GLOBAL STATE #################################

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"

# placeholder for Logger
L = logging.root

###############################################################################

class Controller(RESTController):

    def __init__(self, ctx, addr):
        super().__init__("MD", ctx, addr)
        self._base_url = "https://api.ddex.io/api/v1"

    def _process_fetched_data(self, data : bytes, url):
        data = data.decode()
        data = json.loads(data)
        if not data["ok"]:
            raise Exception("error on request: " + url)
        return data

    @ControllerBase.handler()
    async def get_status(self, ident, msg):
        status = {
            "name": MODULE_NAME,
            "connector_name": "ddex.io",
            "status": g.status,
            "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
        }
        return [status]

    @ControllerBase.handler()
    async def list_directory(self, ident, msg):
        url = self._base_url + "/tickers"
        data = await self._http_(url)
        res = sorted([x["pair"] for x in data["tickers"]])
        res = [dict(name=x.upper(), ticker_id=x) for x in res]
        return res

    @ControllerBase.handler()
    async def get_ticker_info(self, ident, msg):
        content = msg["content"]
        ticker = content.get("ticker")
        ticker_id = ticker.get("ticker_id", ticker.get("symbol", "").lower())
        if ticker_id:
            ticker_id = ticker_id.split("-")[0]
        url = self._base_url + "/tokens"
        data = await self._fetch_cached(url)
        data = data["tokens"]
        if ticker_id:
            data = [x for x in data if x["symbol"] == ticker_id]
            if not data:
                raise Exception("ticker not found")
            assert len(data) == 1, len(data)
        res = []
        for t in data:
            d = {}
            d["description"] = t["name"]
            d["ticker_id"] = t["symbol"] + "-" + "ETH"
            d["price_tick_size"] = 10 ** -t["pricePrecision"]
            d["volume_tick_size"] = t["minimumAmountSize"]
            res.append(d)
        return res

    @ControllerBase.handler()
    async def list_capabilities(self, ident, msg):
        return CAPABILITIES

    async def _get_order_book_snapshot(self, ticker_id, session):
        res = {}
        url = self._base_url + "/books/" + ticker_id
        data = await self._do_fetch(session, url)
        data = data["book"]
        res["bids"] = []
        res["asks"] = []
        if data.get("bids"):
            res["bids"] = [{"price": float(x[0]), "size": float(x[1])}
                           for x in data["bids"]]
        if data.get("asks"):
            res["asks"] = [{"price": float(x[0]), "size": float(x[1])}
                           for x in data["asks"]]
        return res

    async def _get_last_trade(self, ticker_id, session):
        res = {}
        url = self._base_url + "/trades/" + ticker_id
        data = await self._do_fetch(session, url)
        data = data["trades"]
        if data:
            last = data[0]
            res["last_price"] = float(last["price"])
            res["last_size"] = float(last["amount"])
            res["last_timestamp"] = float(last["executedAt"])
        return res

    @ControllerBase.handler()
    async def get_snapshot(self, ident, msg):
        res = {}
        content = msg["content"]
        ticker_id = content["ticker_id"]
        ob_levels = content.get("order_book_levels", 0)
        async with aiohttp.ClientSession() as session:
            ob_data = await self._get_order_book_snapshot(ticker_id, session)
            if ob_data["bids"]:
                best_lvl = ob_data["bids"][0]
                res["bid_price"] = best_lvl["price"]
                res["bid_size"] = best_lvl["size"]
            if ob_data["asks"]:
                best_lvl = ob_data["asks"][0]
                res["ask_price"] = best_lvl["price"]
                res["ask_size"] = best_lvl["size"]
            if ob_levels > 0:
                ob_data["bids"] = ob_data["bids"][:ob_levels]
                ob_data["asks"] = ob_data["asks"][:ob_levels]
                res["order_book"] = ob_data
            last_trade = await self._get_last_trade(ticker_id, session)
            res.update(last_trade)
            if content["daily_data"]:
                url = self._base_url + "/tickers"
                data = await self._fetch_cached(url, expiration_secs=0)
                data = data["tickers"]
                data = [x for x in data if x["pair"] == ticker_id]
                assert len(data) == 1, len(data)
                data = data[0]
                daily = {}
                daily["volume"] = 0
                if data.get("amount24h"):
                    daily["volume"] = float(data["amount24h"])
                if res.get("last_price") and daily["volume"] > 0:
                    change = data["price24h"]
                    change = float(change) if change else 0.0
                    change_mult = 1 + change
                    daily["open"] = res["last_price"] / change_mult
                res["daily"] = daily
        return res

###############################################################################

def parse_args():
    parser = argparse.ArgumentParser(description="ddex.io md connector")
    parser.add_argument("ctl_addr", help="address to bind to for ctl socket")
    parser.add_argument("pub_addr", help="address to bind to for pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def setup_logging(args):
    setup_root_logger(args.log_level)

def main():
    args = parse_args()
    setup_logging(args)
    g.ctl = Controller(g.ctx, args.ctl_addr)
    L.debug("starting event loop ...")
    tasks = [
        create_task(g.ctl.run()),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))
    g.ctx.destroy()

if __name__ == "__main__":
    main()

