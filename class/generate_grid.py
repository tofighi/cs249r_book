import pandas as pd
import requests
from absl import app
import os
import collections


EXCLUDED_USERS = ["profvjreddi", "mpstewart1", "uchendui", "happyappledog"]


def get_pull_requests_with_label(owner, repo, label, token):
    prs_with_label = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all&page={page}"
        headers = {"Authorization": f"token {token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            pull_requests = response.json()
            if not pull_requests:
                break
            for pr in pull_requests:
                if label in [lbl["name"] for lbl in pr.get("labels", [])]:
                    prs_with_label.append(pr)
            page += 1
        else:
            break
    return prs_with_label


def get_comments_for_pull_request(owner, repo, pull_number, token):
    comments = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/comments?page={page}"
        headers = {"Authorization": f"token {token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            page_comments = response.json()
            if not page_comments:
                break
            comments.extend(page_comments)
            page += 1
        else:
            break
    return comments


def main(_):
    owner = "harvard-edge"
    repo = "cs249r_book"
    label = "cs249r"
    token = os.environ.get("GITHUB_TOKEN")
    pull_requests = get_pull_requests_with_label(owner, repo, label, token)

    if os.path.exists("student_comments.json"):
        comments_df = pd.read_json("student_comments.json")
    else:
        comments_data = []
        comments_df = pd.DataFrame(
            columns=["user", "comment", "pr_title", "pr_number", "profile_picture"]
        )

        for pr in pull_requests:
            pr_title = pr["title"]
            pr_number = pr["number"]

            comments = get_comments_for_pull_request(owner, repo, pr_number, token)
            for comment in comments:
                user_name = comment["user"]["login"]
                if user_name in EXCLUDED_USERS:
                    continue

                comment_body = comment["body"]
                profile_picture = comment["user"]["avatar_url"]

                comments_data.append(
                    pd.DataFrame(
                        [
                            [
                                user_name,
                                comment_body,
                                pr_title,
                                pr_number,
                                profile_picture,
                            ]
                        ],
                        columns=[
                            "user",
                            "comment",
                            "pr_title",
                            "pr_number",
                            "profile_picture",
                        ],
                    )
                )
        comments_df = pd.concat(comments_data)
        comments_df.to_json("student_comments.json", orient="records")

    # Let's make a sub dataframe that has the rows being students and the columns being word counts of their comments on each PR
    grouped = (
        comments_df.groupby(["user", "pr_number"])
        .agg({"comment": " ".join})
        .reset_index()
    )
    grouped["word_count"] = grouped["comment"].apply(lambda x: len(x.split()))
    grouped = grouped.sort_values("user")

    # Pivot the table so that the rows are students and the columns are PRs. The values are the word counts of their comments on each PR
    pivoted = grouped.pivot(index="user", columns="pr_number", values="word_count")
    pivoted = pivoted.fillna(0)  # Replace NaNs with 0s

    # Make sure that each PR has a column in case no student commented on it
    for pr in pull_requests:
        pr_number = pr["number"]
        if pr_number not in pivoted.columns:
            pivoted[pr_number] = 0

    # Sort the columns by PR number
    pivoted = pivoted.reindex(sorted(pivoted.columns), axis=1)

    # Print but center the values in the cells and make the datatypes ints!
    pivoted.style.set_properties(**{"text-align": "center"}).set_table_styles(
        [dict(selector="th", props=[("text-align", "center")])]
    ).format("{:.0f}")
    pivoted = pivoted.astype(int)

    # Print it out so i can easily copy to excel with headers in tact
    print(pivoted.to_csv())


if __name__ == "__main__":
    app.run(main)
