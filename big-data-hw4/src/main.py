import numpy as np
from os import path
from scipy import sparse

NODES_COUNT = 7115

PAGERANK_BETA = 0.85
PAGERANK_ITERATIONS = 100

HITS_ITERATIONS = 100

TOP_K = 10


def load_dataset():
    """
    this functions loads dataset and generates link matrix, transition matrix and a id converter to map matrix indices
    to node names in the file and vice versa
    :return:
    """
    l_matrix_rows = []
    l_matrix_columns = []

    degree_list = [0] * NODES_COUNT

    id_converter = {}
    last_mat_pos = 0

    with open(path.join(path.abspath(path.dirname(__file__)), '../data/wiki.data'), 'r') as fp:
        line = fp.readline()

        while line:
            node1_id, node2_id = line.split('\t')
            node1_id = int(node1_id)
            node2_id = int(node2_id)

            if node1_id not in id_converter:
                id_converter[node1_id] = last_mat_pos
                id_converter['r' + str(last_mat_pos)] = node1_id
                last_mat_pos += 1

            if node2_id not in id_converter:
                id_converter[node2_id] = last_mat_pos
                id_converter['r' + str(last_mat_pos)] = node2_id
                last_mat_pos += 1

            node1 = id_converter[node1_id]
            node2 = id_converter[node2_id]

            l_matrix_rows.append(node1)
            l_matrix_columns.append(node2)

            degree_list[node1] += 1

            line = fp.readline()

    matrix_data = [1.0] * len(l_matrix_rows)
    l_matrix = sparse.csr_matrix((matrix_data, (l_matrix_rows, l_matrix_columns)), shape=(NODES_COUNT, NODES_COUNT))

    for i, node in enumerate(l_matrix_rows):
        matrix_data[i] = 1 / degree_list[node]

    m_matrix = sparse.csr_matrix((matrix_data, (l_matrix_rows, l_matrix_columns)), shape=(NODES_COUNT, NODES_COUNT))

    return l_matrix, m_matrix.T, id_converter


def pagerank(m_matrix):
    """
    computes pagerank algorithm on input transition matrix
    :param m_matrix: transition matrix
    :return:
    """
    n = m_matrix.shape[0]

    ranks = np.ones((n, 1)) / n  # initial ranks is same as e/n

    bm_matrix = PAGERANK_BETA * m_matrix  # b*M
    bteleport_matrix = (1 - PAGERANK_BETA) * ranks  # (1-b) * (e/n)
    for i in range(PAGERANK_ITERATIONS):
        ranks = (bm_matrix * ranks) + bteleport_matrix  # v' = bMv + (1-b)e/n

    return ranks  # a n * 1 vector showing pagerank of each node (ith element shows ith node pagerank)


def hits(l_matrix):
    """
    computes hub and authority ranks using hits algorithm and link matrix
    :param l_matrix: link matrix
    :return:
    """
    n = l_matrix.shape[0]

    h = np.ones((n, 1))
    a = np.ones((n, 1))

    l_t_matrix = l_matrix.T

    for i in range(HITS_ITERATIONS):
        a = l_t_matrix * h
        f = a.max()
        if f != 0:
            a = a / f

        h = l_matrix * a
        f = h.max()
        if f != 0:
            h = h / f

    return h, a


def get_top_k(arr, k, id_converter):
    arr2 = np.ravel(arr)
    ind = arr2.argsort()[-k:][::-1]
    res = []
    for i in ind:
        res.append((id_converter['r' + str(i)], arr2[i]))

    return res


if __name__ == '__main__':
    print('Loading Dataset ...')
    l_matrix, m_matrix, id_converter = load_dataset()

    print('Which Algorithm Would you want to use?')
    print('1. Pagerank')
    print('2. Hubs and Authorities')
    algorithm = int(input('Enter Your Choice: '))

    if algorithm == 1:
        ranks = pagerank(m_matrix)

        top_k = get_top_k(ranks, TOP_K, id_converter)

        print('> Top K Page Ranks: ')
        print(top_k)
    else:
        h_ranks, a_ranks = hits(l_matrix)

        top_k_h = get_top_k(h_ranks, TOP_K, id_converter)
        top_k_a = get_top_k(a_ranks, TOP_K, id_converter)

        print('> Top 10 Hubs: ')
        print(top_k_h)
        print('> Top K Authorities: ')
        print(top_k_a)
