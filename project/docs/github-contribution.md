# Checklist
1. Give title for all the plots
2. Remove/comment unnecessary print statements
3. Sort the output data in descending order unless we are finding for the lowest
4. Capture the analysis observation in respective markdown cells (ex: invalid violation code 0 is the top 1 in the frequency)
5. Check for TODO tags

# Add 'upstream' repo to list of remotes
`git remote add upstream git@github.com:MTech-DA231-Project/NYC_Parking_Analysis.git`

# Verify the new remote named 'upstream'
`git remote -v`

# Fetch from upstream remote
`git fetch upstream`

# View all branches, including those from upstream
`git branch -va`

# Checkout your main branch and merge upstream. Never push changes directly to main branch. Always get them from upsteam after raising PR from local dev branch
`git checkout main`

```
git merge upstream/main
git push origin main

OR

git rebase upstream/main
git push -f origin main
```

# Checkout the main branch - you want your new branch to come from main
`git checkout main`

# Create a new branch named newfeature (give your branch its own simple informative name).
`git branch newfeature`

# Switch to your new branch
`git checkout newfeature`

# Commit changes
git commit -am "msg"

# Fetch upstream main and merge with your repo's main branch
```
git fetch upstream
git checkout main
git merge upstream/main
git push origin main
```

# If there were any new commits, rebase your development branch
```
git checkout newfeature
git rebase main
```

  # it may be desirable to squash some of your smaller commits down into a small number of larger more cohesive commits. You can do this with an interactive rebase
  # Rebase all commits on your development branch
  ```
  git checkout
  git rebase -i main
  ```

# Push changes to fork github repo
`git push origin newfeature`

# Use github UI to raise the pull request