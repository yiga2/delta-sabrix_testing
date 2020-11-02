import cx_Oracle
import gzip
import requests
from lxml import etree, html
from datetime import datetime
import os, sys
import asyncio
import time
import aiohttp
import aiofiles
from typing import IO


# see https://realpython.com/async-io-python/

num_tests = 10#00

if os.environ['USERDOMAIN'] != 'DELTA':
    sbxurl1, sbxurl2, sbxdb = 'localhost:8080', 'localhost:8080', 'localhost:9427'
else:
    sbxurl1, sbxurl2, sbxdb = 'bltwi0101la.delta.com:17061', \
                              'bltwd0101la.delta.com:17061', \
                              'ora-blt-db-prd.delta.com:9425'


sql = """ select input_xml, transaction_date, external_company_id, invoice_num 
          from sbxaud.TB_INVOICES
          where input_xml is not null and transaction_date >= DATE '2018-01-01'
          --order by dbms_random.value
          offset 0 rows fetch next :numrows rows only"""


def get_release(host_port):
    x = requests.get(f'http://{host_port}/sabrix/taxproduct')
    root = html.fromstring(x.text)
    xpath = './/tr[td[text()="Sabrix Version Installed"]]/td[2]'  # find row and get second cell
    return root.xpath(xpath)[0].text


async def fetch(session, host_port, indata):
    async with session.post(f'http://{host_port}/sabrix/xmlinvoice', data=indata) as response:
        return await response.read()


async def get_result(session, gz_indata, prefix):
    indata = gzip.decompress(gz_indata).strip()
    root = etree.fromstring(indata)
    nr_lines = root.xpath('count(.//LINE)')
    audit_flag = root.find('.//IS_AUDITED')
    if audit_flag is not None: audit_flag.text = 'N'  # do not save to audit

    indata = etree.tostring(root)

    res = await fetch(session, sbxurl1, indata)
    root = etree.fromstring(res)
    states1 = ', '.join({n.text for n in root.findall('.//TAXABLE_STATE')})  # all distinct values in one string
    tax1 = root.findtext('.//TOTAL_TAX_AMOUNT')

    res = await fetch(session, sbxurl2, indata)
    root = etree.fromstring(res)

    return prefix + [nr_lines, states1, tax1, root.findtext('.//TOTAL_TAX_AMOUNT')]


async def write_one(file, sem, session, gz_indata, prefix):
    async with sem:
        res = await get_result(session, gz_indata, prefix)
        async with aiofiles.open(file, "a") as f:
            res = map(str, res)
            await f.write(','.join(res) + '\n')


async def bulk_crawl_and_write2(file: IO, inputs) -> None:
    sem = asyncio.Semaphore(100)
    conn = aiohttp.TCPConnector(limit=64, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:

        tasks = []
        for xml, prefix in inputs:
            task = write_one(file, sem, session, xml, prefix)
            tasks.append(task)
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    csv_headers = ['calc_timestamp', 'original_date', 'company', 'invoice', 'nr_lines', 'state',
                   get_release(sbxurl1), get_release(sbxurl2)]
    outpath = 'result.csv'
    with open(outpath, 'w') as f:
        f.write(','.join(csv_headers) + '\n')

    start_time = time.time()
    with cx_Oracle.connect('sbxaud','sbxaud', f'{sbxdb}/ORA_SVC_BLT_OPS_PROD') as cn:
        with cn.cursor() as cur:
            inputs = []
            for row in cur.execute(sql, numrows=num_tests):
                inputs.append((row[0].read(), [datetime.now(), row[1].date(), row[2], row[3]]))
    print(f"Recordset read in {time.time() - start_time} seconds")

    start_time = time.time()
    asyncio.run(bulk_crawl_and_write2(outpath, inputs))
    print(f"Calcs written in {time.time() - start_time} seconds")


