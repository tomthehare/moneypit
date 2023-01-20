SELECT cat.Name as CategoryName, SUM(TxDenomination) AS MoneySpent
FROM tblTransaction tx
INNER JOIN tblCategory cat ON tx.TxCategoryID = cat.CategoryID
WHERE cat.Name not in ('transfer', 'credit card payment')
GROUP BY cat.Name
ORDER BY 2 


