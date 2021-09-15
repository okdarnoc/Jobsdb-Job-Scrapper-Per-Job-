import json             # for work with json
import aiohttp          # for async requests to api
import asyncio          # more faster loop
from math import ceil   # ceil - round number up
import time             # for count working time
import pandas           # for save data in csv (actually there have sense change it if we want speed up sctipy
import os               # for check file exist

JOB_PER_PAGE = 30       # Increase if necessary to improve performance
search_queries = ['PHP', 'Senior engineer'] # put your keyword for searching jobs

async def fetch_post(session, url, payload):
    """Send post request to api."""
    async with session.post(url, data=payload) as response:
        return await response.json()


async def parse(session, search_query, page=1):
    print(f'Scraping page: {page}, search_query: {search_query}')
    file_name = search_query + '.csv'
    page_items = []
    url = 'https://xapi.supercharge-srp.co/job-search/graphql?country=hk&isSmartSearch=true'

    payload = json.dumps({
        "query": "query getJobs{jobs(page: %s, locale: \"en\", country: \"hk\", keyword: \"%s\"){total jobs{id "
                 "companyMeta{name}jobTitle "
                 "jobUrl employmentTypes{name}categories{name}careerLevelName "
                 "qualificationName industry{name}workExperienceName}}}" % (page, search_query)
    })
    jobs_response = await fetch_post(session,url, payload=payload) # send post request and got response

    jobs = jobs_response["data"]["jobs"]["jobs"] # parsing response (all jobs on the current page)
    for job in jobs:
        item = {
            "job_id": job.get("id"),
            "job_title": job.get("jobTitle"),
            "company": job.get("companyMeta", {}).get("name"),
            "job_function": ", ".join([category["name"] for category in job.get("categories", [])]),
            "job_type": ", ".join(
                [employment_type.get("name") for employment_type in job.get("employmentTypes", [])]),
            "industry": job.get("industry", {}).get("name"),
            "career_level": job.get("careerLevelName"),
            "years_of_experience": job.get("workExperienceName"),
            "qualification": job.get("qualificationName")
        }
        job_id = job.get("id")

        payload_job  = json.dumps({
            "query": "query getJobDetail{jobDetail(jobId: \"%s\", locale: \"en\", country: \"hk\")"
                     "{jobDetail {jobRequirement {benefits}}}}" % job_id
        })

        response_job_detail = await fetch_post(session,url,payload_job) # one more post request, for get job detail
        job = response_job_detail.get("data").get("jobDetail")
        item["benefits"] = ", ".join(job.get("jobDetail").get("jobRequirement").get("benefits"))

        page_items.append(item) # add item to page_items list

    df = pandas.DataFrame.from_dict(page_items) # create DataFrame (table) from page_items. create a table from the data collected from the page
    df.to_csv(file_name, mode='a', index=None, header=not os.path.exists(file_name)) # write table to csv.  mode 'a' append info to file if exsist (not create new file)

    total_jobs = jobs_response["data"]["jobs"]["total"]
    total_pages = ceil(total_jobs / JOB_PER_PAGE)
    # if current page < total_pages. increase current page + 1 , and parse this page ...
    # if current page == total_pages, leave function. We pull all data
    if page < total_pages:
        page += 1
        await parse(session, search_query, page)


async def main():
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "*/*",
        "Accept-Language": "ru,en-US;q=0.7,en;q=0.3",
        "content-type": "application/json"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        # create session here for saved headers for all next requests
        tasks = [parse(session, search_query) for search_query in search_queries] # create tasks every task run separate
        return await asyncio.gather(*tasks) # run tasks


if __name__ == '__main__':
    start_time = time.time()    # start track time
    asyncio.run(main()) # run main() function as async function
    print("--- %s seconds ---" % (time.time() - start_time))    # got working time

