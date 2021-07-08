import configparser as cp

if __name__ == '__main__':
    cf = cp.ConfigParser()
    cf['FILES'] = {'db': 'stock.sqlite'}
    with open('app/app.cfg', 'w') as f:
        cf.write(f)