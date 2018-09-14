import numpy as np


class Sampler(object):

    @staticmethod
    def calculate_accumulative_sum(input_list):
        cnt = 0
        result = []
        for sample in input_list:
            cnt += int(sample)
            result.append(cnt)
        return result

    def get_exponential_samples(self, lambd=30, size=100, acc=True):
        poisson_samples = np.random.poisson(lam=lambd, size=size)
        if not acc:
            samples = poisson_samples
        else:
            samples = self.calculate_accumulative_sum(poisson_samples)
        return samples

    @staticmethod
    def get_normal_sample(mean=120, std=10):
        return int(np.random.normal(mean, std))

    @staticmethod
    def get_uniform_sample(low, high):
        return int(np.random.uniform(low, high))

    def get_triangular_samples(self, left, mode, right, size=60):
        triangular_samples = np.random.triangular(left, mode, right, size)
        samples = self.calculate_accumulative_sum(triangular_samples)
        return samples


sampler = Sampler()
